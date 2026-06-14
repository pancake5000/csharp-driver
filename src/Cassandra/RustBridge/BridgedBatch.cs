using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Cassandra.Serialization;
using static Cassandra.RustBridge;

namespace Cassandra
{
    /// <summary>
    /// Bridges a Rust-owned batch resource to C#.
    /// </summary>
    /// <remarks>
    /// The batch is assembled incrementally: <see cref="Create"/> allocates an empty
    /// Rust batch, then one <c>Append*</c> call per child statement serializes that
    /// statement's values (via the populate-values callback) and appends it on the Rust
    /// side. The assembled batch is then executed with <see cref="BridgedSession.Batch"/>.
    /// </remarks>
    internal sealed class BridgedBatch : RustResource
    {
        internal BridgedBatch(ManuallyDestructible mdBatch) : base(mdBatch)
        {
        }

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern FFIMaybeException batch_create(byte batchType, out ManuallyDestructible outBatch, IntPtr constructorsPtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern FFIMaybeException batch_append_simple(IntPtr batch, [MarshalAs(UnmanagedType.LPUTF8Str)] string statement, IntPtr populateValuesContext, IntPtr populateValuesCallback);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern FFIMaybeException batch_append_prepared(IntPtr batch, IntPtr preparedStatement, IntPtr populateValuesContext, IntPtr populateValuesCallback);

        /// <summary>
        /// Creates a new, empty batch of the given type.
        /// </summary>
        internal static BridgedBatch Create(BatchType batchType)
        {
            ManuallyDestructible md = default;
            FFIMaybeException exception;
            unsafe
            {
                exception = batch_create((byte)batchType, out md, (IntPtr)Globals.ConstructorsPtr);
            }

            // batch_create is not tied to an existing handle, so the exception is
            // checked here rather than through RustResource.RunWithIncrement.
            try
            {
                ThrowIfException(ref exception);
            }
            finally
            {
                FreeExceptionHandle(ref exception);
            }

            return new BridgedBatch(md);
        }

        /// <summary>
        /// Appends a simple (unprepared) statement and its values to the batch.
        /// </summary>
        internal unsafe void AppendSimple(string statement, object[] values, ISerializer serializer)
        {
            var populateCtx = SerializationHandler.CreateContext(values, serializer);
            var ctxIntPtr = (IntPtr)Unsafe.AsPointer(ref populateCtx);
            RunWithIncrement(handle =>
                batch_append_simple(
                    handle,
                    statement,
                    ctxIntPtr,
                    (IntPtr)SerializationHandler.PopulateValuesPtr));
            GC.KeepAlive(populateCtx);
        }

        /// <summary>
        /// Appends a prepared statement (referenced by its native handle) and its bound
        /// values to the batch.
        /// </summary>
        internal unsafe void AppendPrepared(IntPtr preparedStatement, object[] values, ISerializer serializer)
        {
            var populateCtx = SerializationHandler.CreateContext(values, serializer);
            var ctxIntPtr = (IntPtr)Unsafe.AsPointer(ref populateCtx);
            RunWithIncrement(handle =>
                batch_append_prepared(
                    handle,
                    preparedStatement,
                    ctxIntPtr,
                    (IntPtr)SerializationHandler.PopulateValuesPtr));
            GC.KeepAlive(populateCtx);
        }
    }
}
