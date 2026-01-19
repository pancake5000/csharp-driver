using System;

namespace Cassandra
{
    /// <summary>
    /// Exception that indicates that the index is out of range when accessing collection types.
    /// </summary>
    public class IndexOutOfRangeException : DriverException
    {
        public IndexOutOfRangeException(string message) : base(message, null)
        { }

    }
}
