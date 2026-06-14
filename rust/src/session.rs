use std::convert::Infallible;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;

use scylla::client::session::Session;
use scylla::cluster::ClusterState;
use scylla::errors::{ExecutionError, NewSessionError, PagerExecutionError, PrepareError};
use scylla::statement::Statement;
use scylla_cql::deserialize::result::RawRowLendingIterator;
use scylla_cql::serialize::row::SerializedValues;
use tokio::sync::RwLock;

use crate::batch::BridgedBatch;
use crate::error_conversion::{FFIMaybeException, SessionOperationError};
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, CSharpManagedStringPtr, CSharpStr,
    FFI, FFIBool, FFIStr, FromArc, WriteStringCallback,
};
use crate::pre_serialized_values::{PopulateValues, PopulateValuesContext, PreSerializedValues};
use crate::prepared_statement::BridgedPreparedStatement;
use crate::row_set::RowSet;
use crate::session_config::BridgedSessionConfig;
use crate::task::{BridgedFuture, ExceptionConstructors, ManuallyDestructible, Tcb};

/// Internal representation of a session bridged to C#.
/// It contains optional connected session state to allow for shutdown.
/// If None, the session has been shut down and cannot be used for queries.
#[derive(Debug)]
pub(crate) struct BridgedSessionInner {
    session: Option<Session>,
}

/// Execution options for bound statements mirrored with the managed FFI struct.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BoundStatementExecutionOptions {
    pub consistency_level: u16,
    pub has_consistency_level: FFIBool,
    pub is_idempotent: FFIBool,
    pub page_size: i32,
}

/// Execution options for simple (unprepared) statements mirrored with
/// the managed FFI struct.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SimpleStatementExecutionOptions {
    pub consistency_level: u16,
    pub has_consistency_level: FFIBool,
    pub is_idempotent: FFIBool,
    pub page_size: i32,
}

/// BridgedSession is a thread-safe, asynchronously accessible session wrapper.
/// It uses RwLock to allow multiple concurrent read accesses (queries)
/// while ensuring exclusive access for write operations (shutdown).
pub type BridgedSession = tokio::sync::RwLock<BridgedSessionInner>;
impl FFI for BridgedSession {
    type Origin = FromArc;
}

/// BridgedFuture currently needs to return some result that implements ArcFFI.
/// For operations that don't need to return any data we use EmptyBridgedResult.
/// The user must call empty_bridged_result_free after using such functions.
#[derive(Debug)]
pub struct EmptyBridgedResult;
impl FFI for EmptyBridgedResult {
    type Origin = FromArc;
}

#[unsafe(no_mangle)]
pub extern "C" fn empty_bridged_result_free(ptr: BridgedOwnedSharedPtr<EmptyBridgedResult>) {
    ArcFFI::free(ptr);
    tracing::trace!("[FFI] EmptyBridgedResult freed");
}

#[unsafe(no_mangle)]
pub extern "C" fn session_create(tcb: Tcb<ManuallyDestructible>, config: BridgedSessionConfig<'_>) {
    let (uri, keyspace, session_builder) = config.into_session_builder();
    let uri = uri.to_owned();
    let keyspace = keyspace.to_owned();

    BridgedFuture::spawn::<_, _, NewSessionError, _>(tcb, async move {
        tracing::debug!("[FFI] Create Session... {}", uri);

        let session = session_builder.build().await?;

        tracing::info!(
            "[FFI] Session created! URI: {}, Keyspace: {}",
            uri,
            keyspace
        );
        tracing::trace!(
            "[FFI] Contacted node's address: {}",
            session.get_cluster_state().get_nodes_info()[0].address
        );

        Ok(Arc::new(RwLock::new(BridgedSessionInner {
            session: Some(session),
        })))
    })
}

/// Shuts down the session by acquiring a write lock and clearing the connected state.
/// This blocks all future queries. Once shutdown, the session cannot be used for queries anymore.
#[unsafe(no_mangle)]
pub extern "C" fn session_shutdown(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
) {
    // Session pointer being null or invalid implies a serious error on the C# side.
    // We unwrap here to catch such issues early and panic.
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling session shutdown");

    BridgedFuture::spawn::<_, _, Infallible, _>(tcb, async move {
        tracing::debug!("[FFI] Shutting down session");

        // Acquire write lock - this will pause the asynchronous execution until all read locks (queries)
        // are released and then clear the connected state - no more queries can proceed after this.
        let mut session_guard = session_arc.write().await;

        if session_guard.session.is_none() {
            panic!("Session is already shut down");
        }

        session_guard.session = None;
        tracing::info!("[FFI] Session shutdown complete");

        // Return None, providing BridgedSession just to satisfy the type constraints.
        // This is temporary and will be replaced with a proper non-allocating empty result type.
        Ok(None::<Arc<BridgedSession>>)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    execution_options: SimpleStatementExecutionOptions,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for execution: \"{}\"",
        statement
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, SessionOperationError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let mut statement = Statement::new(statement);
        statement.set_is_idempotent(bool::from(execution_options.is_idempotent));
        statement.set_page_size(execution_options.page_size);

        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for simple query: {1}",
                        execution_options.consistency_level, err
                    ))
                })?;
            statement.set_consistency(consistency);
        } else {
            statement.unset_consistency();
        }

        // Lock is held for the entire duration of the query operation,
        // preventing shutdown until this future completes
        // Map underlying `PagerExecutionError` into `SessionOperationError::Inner` so
        // the BridgedFuture's error type matches.
        let query_pager = session
            .query_iter(statement, ())
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Statement executed");

        Ok(Arc::new(RowSet::Paged(tokio::sync::Mutex::new(
            query_pager,
        ))))
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_with_values(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    populate_values_context: PopulateValuesContext<'_>,
    populate_values: PopulateValues,
    execution_options: SimpleStatementExecutionOptions,
) {
    let psv =
        match PreSerializedValues::from_populate_callback(populate_values_context, populate_values)
        {
            Ok(v) => v,
            Err(exception) => {
                tcb.fail_task(exception);
                return;
            }
        };

    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    //TODO: use safe error propagation mechanism

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, SessionOperationError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!(
            "[FFI] Preparing and executing statement with pre-serialized values \"{}\"",
            statement
        );

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // First, prepare the statement. Map PrepareError into PagerExecutionError::PrepareError
        // and then into SessionOperationError::Inner so the error type matches.
        let mut prepared = session
            .prepare(statement)
            .await
            .map_err(|e| SessionOperationError::Inner(PagerExecutionError::PrepareError(e)))?;

        prepared.set_is_idempotent(bool::from(execution_options.is_idempotent));
        prepared.set_page_size(execution_options.page_size);

        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for simple query with values: {1}",
                        execution_options.consistency_level, err
                    ))
                })?;
            prepared.set_consistency(consistency);
        } else {
            prepared.unset_consistency();
        }

        // Convert our FFI wrapper into SerializedValues by consuming it.
        let serialized_values: SerializedValues = psv.into_serialized_values();

        // Now execute using the internal execute_iter_preserialized helper.
        // Map to appropriate error type.
        let query_pager = session
            .execute_iter_preserialized(prepared, serialized_values)
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed with pre-serialized values");

        Ok(Arc::new(RowSet::Paged(tokio::sync::Mutex::new(
            query_pager,
        ))))
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_prepare(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
) {
    // Convert the raw C string to a Rust string.
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling statement for preparation: \"{}\"",
        statement
    );

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, SessionOperationError<PrepareError>, _>(tcb, async move {
        tracing::debug!("[FFI] Preparing statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Lock is held for the entire duration of the prepare operation,
        // preventing shutdown until this future completes
        // Map underlying `PrepareError` into `SessionOperationError::Inner` so
        // the BridgedFuture's error type matches.
        let ps = session
            .prepare(statement)
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Statement prepared");

        Ok(Arc::new(BridgedPreparedStatement {
            inner: StdRwLock::new(ps),
        }))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    execution_options: BoundStatementExecutionOptions,
) {
    let bridged_prepared = ArcFFI::as_ref(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling prepared statement execution");

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    // Clone the prepared statement to move it into the async task.
    // On the cloned prepared statement, set the execution options (consistency level and idempotence) before executing.
    let mut prepared_statement = bridged_prepared
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics")
        .clone();

    BridgedFuture::spawn::<_, _, SessionOperationError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing prepared statement");

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // If consistency level was provided, apply it to the prepared statement. Otherwise, if consistency level
        // was not provided, ensure it's unset on the prepared statement so it uses the default (between creating
        // this bound statement and executing it someone could set a consistency level on the prepared statement).
        // NOTE: logic used here for applying consistency level is complicated and for now there is no tests covering it.
        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for bound query: {1}",
                        execution_options.consistency_level, err
                    ))
                })?;
            prepared_statement.set_consistency(consistency);
        } else {
            prepared_statement.unset_consistency();
        }

        prepared_statement.set_is_idempotent(bool::from(execution_options.is_idempotent));
        prepared_statement.set_page_size(execution_options.page_size);

        // Lock is held for the entire duration of the query operation,
        // preventing shutdown until this future completes
        // Map underlying `PagerExecutionError` into `SessionOperationError::Inner` so
        // the BridgedFuture's error type matches.
        let query_pager = session
            .execute_iter(prepared_statement, ())
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed");

        Ok(Arc::new(RowSet::Paged(tokio::sync::Mutex::new(
            query_pager,
        ))))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound_with_values(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    populate_values_context: PopulateValuesContext<'_>,
    populate_values: PopulateValues,
    execution_options: BoundStatementExecutionOptions,
) {
    let psv =
        match PreSerializedValues::from_populate_callback(populate_values_context, populate_values)
        {
            Ok(v) => v,
            Err(exception) => {
                tcb.fail_task(exception);
                return;
            }
        };

    let bridged_prepared = ArcFFI::as_ref(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling prepared statement execution");

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    // Clone the prepared statement to move it into the async task.
    // On the cloned prepared statement, set the execution options (consistency level and idempotence) before executing.
    let mut prepared_statement = bridged_prepared
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics")
        .clone();

    BridgedFuture::spawn::<_, _, SessionOperationError<PagerExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing prepared statement");

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // If consistency level was provided, apply it to the prepared statement. Otherwise, if consistency level
        // was not provided, ensure it's unset on the prepared statement so it uses the default (between creating
        // this bound statement and executing it someone could set a consistency level on the prepared statement).
        // NOTE: logic used here for applying consistency level is complicated and for now there is no tests covering it.
        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for bound query with values: {1}",
                        execution_options.consistency_level,
                        err
                    ))
                })?;
            prepared_statement.set_consistency(consistency);
        } else {
            prepared_statement.unset_consistency();
        }

        prepared_statement.set_is_idempotent(bool::from(execution_options.is_idempotent));
        prepared_statement.set_page_size(execution_options.page_size);

        // Convert our FFI wrapper into SerializedValues by consuming it.
        let serialized_values: SerializedValues = psv.into_serialized_values();

        let query_pager = session
            .execute_iter_preserialized(prepared_statement, serialized_values)
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Prepared statement executed");

        Ok(Arc::new(RowSet::Paged(tokio::sync::Mutex::new(
            query_pager,
        ))))
    });
}

/// Converts a `QueryResult` into a `RowSet::Materialized`, or `None` if the
/// result contained no rows (e.g. DDL / DML statements).
fn query_result_to_materialized_row_set(
    result: scylla::response::query_result::QueryResult,
) -> Option<Arc<RowSet>> {
    let raw_rows = result.into_csharp_raw_rows()?;
    let iter = RawRowLendingIterator::new(raw_rows);
    Some(Arc::new(RowSet::Materialized(std::sync::Mutex::new(iter))))
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_unpaged(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    execution_options: SimpleStatementExecutionOptions,
) {
    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!(
        "[FFI] Scheduling unpaged statement for execution: \"{}\"",
        statement
    );

    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, SessionOperationError<ExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing unpaged statement \"{}\"", statement);

        let Ok(session_guard) = session_guard_res else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let mut stmt = Statement::new(statement);
        stmt.set_is_idempotent(bool::from(execution_options.is_idempotent));

        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for unpaged simple query: {1}",
                        execution_options.consistency_level, err
                    ))
                })?;
            stmt.set_consistency(consistency);
        } else {
            stmt.unset_consistency();
        }

        let result = session
            .query_unpaged(stmt, ())
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Unpaged statement executed");

        Ok(query_result_to_materialized_row_set(result))
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_with_values_unpaged(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    statement: CSharpStr<'_>,
    populate_values_context: PopulateValuesContext<'_>,
    populate_values: PopulateValues,
    execution_options: SimpleStatementExecutionOptions,
) {
    let psv =
        match PreSerializedValues::from_populate_callback(populate_values_context, populate_values)
        {
            Ok(v) => v,
            Err(exception) => {
                tcb.fail_task(exception);
                return;
            }
        };

    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, SessionOperationError<ExecutionError>, _>(tcb, async move {
        tracing::debug!(
            "[FFI] Preparing and executing unpaged statement with pre-serialized values \"{}\"",
            statement
        );

        let Ok(session_guard) = session_guard_res else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let mut prepared = session
            .prepare(statement)
            .await
            .map_err(|e| SessionOperationError::Inner(ExecutionError::PrepareError(e)))?;

        prepared.set_is_idempotent(bool::from(execution_options.is_idempotent));

        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for unpaged simple query with values: {1}",
                        execution_options.consistency_level, err
                    ))
                })?;
            prepared.set_consistency(consistency);
        } else {
            prepared.unset_consistency();
        }

        let serialized_values: SerializedValues = psv.into_serialized_values();

        let result = session
            .execute_unpaged_preserialized(&prepared, serialized_values)
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Unpaged prepared statement executed with pre-serialized values");

        Ok(query_result_to_materialized_row_set(result))
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound_unpaged(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    execution_options: BoundStatementExecutionOptions,
) {
    let bridged_prepared = ArcFFI::as_ref(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling unpaged prepared statement execution");

    let session_guard_res = session_arc.try_read_owned();

    let mut prepared_statement = bridged_prepared
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics")
        .clone();

    BridgedFuture::spawn::<_, _, SessionOperationError<ExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing unpaged prepared statement");

        let Ok(session_guard) = session_guard_res else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for unpaged bound query: {1}",
                        execution_options.consistency_level, err
                    ))
                })?;
            prepared_statement.set_consistency(consistency);
        } else {
            prepared_statement.unset_consistency();
        }

        prepared_statement.set_is_idempotent(bool::from(execution_options.is_idempotent));

        let result = session
            .execute_unpaged(&prepared_statement, ())
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Unpaged prepared statement executed");

        Ok(query_result_to_materialized_row_set(result))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn session_query_bound_with_values_unpaged(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    populate_values_context: PopulateValuesContext<'_>,
    populate_values: PopulateValues,
    execution_options: BoundStatementExecutionOptions,
) {
    let psv =
        match PreSerializedValues::from_populate_callback(populate_values_context, populate_values)
        {
            Ok(v) => v,
            Err(exception) => {
                tcb.fail_task(exception);
                return;
            }
        };

    let bridged_prepared = ArcFFI::as_ref(prepared_statement_ptr).unwrap();
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling unpaged prepared statement execution with values");

    let session_guard_res = session_arc.try_read_owned();

    let mut prepared_statement = bridged_prepared
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics")
        .clone();

    BridgedFuture::spawn::<_, _, SessionOperationError<ExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing unpaged prepared statement with values");

        let Ok(session_guard) = session_guard_res else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        if bool::from(execution_options.has_consistency_level) {
            let consistency = execution_options
                .consistency_level
                .try_into()
                .map_err(|err| {
                    SessionOperationError::InvalidArgument(format!(
                        "Invalid consistency level value {0} passed from C# for unpaged bound query with values: {1}",
                        execution_options.consistency_level,
                        err
                    ))
                })?;
            prepared_statement.set_consistency(consistency);
        } else {
            prepared_statement.unset_consistency();
        }

        prepared_statement.set_is_idempotent(bool::from(execution_options.is_idempotent));

        let serialized_values: SerializedValues = psv.into_serialized_values();

        let result = session
            .execute_unpaged_preserialized(&prepared_statement, serialized_values)
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Unpaged prepared statement executed with values");

        Ok(query_result_to_materialized_row_set(result))
    });
}

/// Executes a batch that was assembled by C# via `batch_create` / `batch_append_*`.
///
/// Batches return no rows (the MVP discards the `QueryResult`), so the task
/// completes with an empty result the same way `session_shutdown` does. The
/// caller receives an empty `ManuallyDestructible` to dispose.
#[unsafe(no_mangle)]
pub extern "C" fn session_batch(
    tcb: Tcb<ManuallyDestructible>,
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    batch_ptr: BridgedBorrowedSharedPtr<'_, BridgedBatch>,
) {
    let session_arc = ArcFFI::cloned_from_ptr(session_ptr).unwrap();
    let batch_arc = ArcFFI::cloned_from_ptr(batch_ptr).unwrap();

    tracing::trace!("[FFI] Scheduling batch execution");

    // Try to acquire an owned read lock.
    // If the operation fails, treat it as session shutting down.
    let session_guard_res = session_arc.try_read_owned();

    BridgedFuture::spawn::<_, _, SessionOperationError<ExecutionError>, _>(tcb, async move {
        tracing::debug!("[FFI] Executing batch");

        let Ok(session_guard) = session_guard_res else {
            // Session is currently shutting down - exit with appropriate error.
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Check if session is connected or if it has been shut down.
        // If it has been shut down, return appropriate error.
        let Some(session) = session_guard.session.as_ref() else {
            return Err(SessionOperationError::AlreadyShutdown);
        };

        // Clone the assembled batch and its values out of the shared handle so
        // they can be moved into (and outlive) this async task.
        let (batch, values) = {
            let guard = batch_arc
                .inner
                .lock()
                .expect("poisoning impossible due to process-aborting panics");
            (guard.batch.clone(), guard.values.clone())
        };

        // Execute the batch with the already-serialized values, avoiding a
        // re-serialization round-trip. The QueryResult is intentionally
        // discarded: batches return no rows in this MVP (conditional/LWT
        // batches lose their `[applied]` column until the materialized-result
        // path is bridged).
        let _result = session
            .batch_preserialized(&batch, &values)
            .await
            .map_err(SessionOperationError::Inner)?;

        tracing::trace!("[FFI] Batch executed");

        // Return an empty result, providing the type just to satisfy the type constraints.
        Ok(None::<Arc<EmptyBridgedResult>>)
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn session_get_keyspace(
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    write_cs_str: WriteStringCallback,
    cs_string: CSharpManagedStringPtr,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let session_arc =
        ArcFFI::as_ref(session_ptr).expect("valid and non-null BridgedSession pointer");

    // Try to acquire a read lock synchronously.
    let Ok(session_guard) = session_arc.try_read() else {
        // Session is currently shutting down.
        let ex = constructors
            .already_shutdown_exception_constructor
            .construct_from_rust("Session has been shut down and can no longer execute operations");
        return FFIMaybeException::from_exception(ex);
    };

    // Check if session is connected or if it has been shut down.
    let Some(session) = session_guard.session.as_ref() else {
        let ex = constructors
            .already_shutdown_exception_constructor
            .construct_from_rust("Session has been shut down and can no longer execute operations");
        return FFIMaybeException::from_exception(ex);
    };

    let Some(keyspace) = session.get_keyspace() else {
        // If no keyspace is set, we don't set FFIStr.
        // This will be treated as null on the C# side.
        return FFIMaybeException::ok();
    };

    let ffi_str = FFIStr::new(keyspace.as_ref());
    write_cs_str(ffi_str, cs_string)
}

/// Sets `out_cluster_state` to the current cluster state as a ManuallyDestructible resource.
/// This function provides access to the cluster topology information from the session.
/// The returned ClusterState is a snapshot at the time of the call.
///
/// # Safety
/// - The session pointer must be valid and not freed
#[unsafe(no_mangle)]
pub extern "C" fn session_get_cluster_state(
    session_ptr: BridgedBorrowedSharedPtr<'_, BridgedSession>,
    out_cluster_state: *mut ManuallyDestructible,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let session_arc =
        ArcFFI::as_ref(session_ptr).expect("valid and non-null BridgedSession pointer");

    // Try to acquire a read lock synchronously.
    let Ok(session_guard) = session_arc.try_read() else {
        // Session is currently shutting down.
        let ex = constructors
            .already_shutdown_exception_constructor
            .construct_from_rust("Session has been shut down and can no longer execute operations");
        return FFIMaybeException::from_exception(ex);
    };

    // Check if session is connected or if it has been shut down.
    let Some(session) = session_guard.session.as_ref() else {
        let ex = constructors
            .already_shutdown_exception_constructor
            .construct_from_rust("Session has been shut down and can no longer execute operations");
        return FFIMaybeException::from_exception(ex);
    };

    // Get the cluster state from the session and convert it into an ArcFFI-wrapped pointer.
    let cluster_state = session.get_cluster_state();
    let md = ManuallyDestructible::from_destructible::<ClusterState>(cluster_state);
    unsafe {
        *out_cluster_state = md;
    }
    FFIMaybeException::ok()
}
