using System.Runtime.InteropServices;
namespace Cassandra
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct BridgedLoadBalancingPolicy
    {
        internal bool isTokenAware;
        internal bool isDCAware;
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        internal string localDC;
    }
}