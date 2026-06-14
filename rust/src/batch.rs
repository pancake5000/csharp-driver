use std::sync::Arc;
use std::sync::Mutex;

use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla_cql::serialize::row::SerializedValues;

use crate::error_conversion::FFIMaybeException;
use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, CSharpStr, FFI, FromArc};
use crate::pre_serialized_values::{PopulateValues, PopulateValuesContext, PreSerializedValues};
use crate::prepared_statement::BridgedPreparedStatement;
use crate::task::{ExceptionConstructors, ManuallyDestructible};

/// A CQL batch being assembled by C# before execution.
///
/// C# builds it incrementally (`batch_create` then one `batch_append_*` call
/// per child statement), then hands it back to `session_batch` for execution.
/// The statements and their pre-serialized values are kept in lockstep: the
/// i-th entry of `values` holds the values for the i-th statement of `batch`.
pub(crate) struct BridgedBatch {
    pub(crate) inner: Mutex<BridgedBatchInner>,
}

pub(crate) struct BridgedBatchInner {
    pub(crate) batch: Batch,
    pub(crate) values: Vec<SerializedValues>,
}

impl FFI for BridgedBatch {
    type Origin = FromArc;
}

/// Creates an empty batch of the given type and returns it as a
/// `ManuallyDestructible` resource owned by C#.
///
/// `batch_type` must be one of: 0 = Logged, 1 = Unlogged, 2 = Counter.
#[unsafe(no_mangle)]
pub extern "C" fn batch_create(
    batch_type: u8,
    out_batch: *mut ManuallyDestructible,
    constructors: &'static ExceptionConstructors,
) -> FFIMaybeException {
    let batch_type = match batch_type {
        0 => BatchType::Logged,
        1 => BatchType::Unlogged,
        2 => BatchType::Counter,
        other => {
            let ex = constructors
                .invalid_argument_exception_constructor
                .construct_from_rust(
                    format!("Invalid batch type value {other} passed from C#.").as_str(),
                );
            return FFIMaybeException::from_exception(ex);
        }
    };

    let bridged = Arc::new(BridgedBatch {
        inner: Mutex::new(BridgedBatchInner {
            batch: Batch::new(batch_type),
            values: Vec::new(),
        }),
    });

    unsafe {
        *out_batch = ManuallyDestructible::from(bridged);
    }

    FFIMaybeException::ok()
}

/// Appends a simple (unprepared) statement and its pre-serialized values to the batch.
///
/// The values are built synchronously via the populate-values callback, exactly
/// like the single-statement `session_query_with_values` path.
#[unsafe(no_mangle)]
pub extern "C" fn batch_append_simple(
    batch_ptr: BridgedBorrowedSharedPtr<'_, BridgedBatch>,
    statement: CSharpStr<'_>,
    populate_values_context: PopulateValuesContext<'_>,
    populate_values: PopulateValues,
) -> FFIMaybeException {
    let psv =
        match PreSerializedValues::from_populate_callback(populate_values_context, populate_values)
        {
            Ok(v) => v,
            Err(exception) => return FFIMaybeException::from_exception(exception),
        };

    let statement = statement.as_cstr().unwrap().to_str().unwrap().to_owned();

    let bridged = ArcFFI::as_ref(batch_ptr).expect("valid and non-null BridgedBatch pointer");
    let mut guard = bridged
        .inner
        .lock()
        .expect("poisoning impossible due to process-aborting panics");

    guard.batch.append_statement(Statement::new(statement));
    guard.values.push(psv.into_serialized_values());

    FFIMaybeException::ok()
}

/// Appends a prepared statement and its pre-serialized values to the batch.
///
/// The prepared statement is cloned out of the bridged handle, mirroring
/// `session_query_bound_with_values`.
#[unsafe(no_mangle)]
pub extern "C" fn batch_append_prepared(
    batch_ptr: BridgedBorrowedSharedPtr<'_, BridgedBatch>,
    prepared_statement_ptr: BridgedBorrowedSharedPtr<'_, BridgedPreparedStatement>,
    populate_values_context: PopulateValuesContext<'_>,
    populate_values: PopulateValues,
) -> FFIMaybeException {
    let psv =
        match PreSerializedValues::from_populate_callback(populate_values_context, populate_values)
        {
            Ok(v) => v,
            Err(exception) => return FFIMaybeException::from_exception(exception),
        };

    let bridged_prepared = ArcFFI::as_ref(prepared_statement_ptr)
        .expect("valid and non-null BridgedPreparedStatement pointer");
    let prepared = bridged_prepared
        .inner
        .read()
        .expect("poisoning impossible due to process-aborting panics")
        .clone();

    let bridged = ArcFFI::as_ref(batch_ptr).expect("valid and non-null BridgedBatch pointer");
    let mut guard = bridged
        .inner
        .lock()
        .expect("poisoning impossible due to process-aborting panics");

    guard.batch.append_statement(prepared);
    guard.values.push(psv.into_serialized_values());

    FFIMaybeException::ok()
}
