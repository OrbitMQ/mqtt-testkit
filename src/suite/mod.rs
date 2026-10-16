// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! The conformance cases, one directory per protocol version and one file
//! per area. `build.rs` registers every top-level `pub fn` of an area file as
//! a case (see [`crate::registry`]); helpers are private or live in `mod.rs`.

pub(crate) mod v3_1;
pub(crate) mod v3_1_1;
pub(crate) mod v5;
