pub mod dynamic_gauge;
mod gauge_struct;
pub mod static_gauge;

pub use dynamic_gauge::{DynamicGauge, DynamicGaugeStorage};
pub use gauge_struct::{GaugeHandleOwned, GaugeInner, Many, One};
pub use static_gauge::StaticGauge;
