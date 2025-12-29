using System.Runtime.InteropServices;

namespace Cassandra
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct BridgedConfiguration
    {
        internal BridgedLoadBalancingPolicy loadBalancingPolicy;
    }
}
