use crate::ffi::{ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FromArc};
use scylla::cluster::ClusterState;
use std::ffi::c_void;

impl FFI for ClusterState {
    type Origin = FromArc;
}

/// Frees a ClusterState pointer obtained from `session_get_cluster_state`.
///
/// # Safety
/// - Must only be called once per pointer
/// - The pointer must have been obtained from `session_get_cluster_state`
/// - After calling this function, the pointer is invalid and must not be used
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_free(cluster_state_ptr: BridgedOwnedSharedPtr<ClusterState>) {
    ArcFFI::free(cluster_state_ptr);
    tracing::trace!("[FFI] ClusterState pointer freed");
}

/// This function returns the raw memory address of the underlying ClusterState object
/// for comparison purposes. The returned address can be stored and compared
/// with addresses from other ClusterState pointers to detect topology changes.
///
/// The returned address is ONLY valid for comparison while the Arc<ClusterState> is alive.
///
/// # Safety
/// - The pointer must be valid and not freed
/// - The returned address must only be used for comparison, never dereferenced
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_raw_ptr(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
) -> *const c_void {
    cluster_state_ptr
        .to_raw()
        .map(|p| p as *const c_void)
        .unwrap_or(std::ptr::null())
}
