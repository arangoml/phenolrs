#[derive(Eq, PartialEq, Copy, Clone)]
pub enum LoadStrategy {
    Dump,
    AQL,
}
