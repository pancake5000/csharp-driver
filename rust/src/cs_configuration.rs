use crate::cs_load_balancing_policy::CSLoadBalancingPolicy;
pub struct CSConfiguration<'a>{
    pub load_balancing_policy: CSLoadBalancingPolicy<'a>
}