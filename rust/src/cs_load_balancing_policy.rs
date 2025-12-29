use crate::CSharpStr;

pub struct CSLoadBalancingPolicy<'a> {
    pub is_token_aware: bool,
    pub is_dc_aware: bool,
    pub local_dc: CSharpStr<'a>,
}
