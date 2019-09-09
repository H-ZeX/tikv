// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryFrom;
use std::{i64, str, u64};

use num_traits::identities::Zero;
use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
use crate::codec::mysql::Time;
use crate::codec::Error;
use crate::expr::{EvalContext, Flag};
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use crate::Result;

fn get_cast_fn_rpn_meta(
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<RpnFnMeta> {
    let from = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
        //  any as real
        (EvalType::Int, EvalType::Real) => {
            let fu = from_field_type.is_unsigned();
            let ru = to_field_type.is_unsigned();
            match (fu, ru) {
                (true, _) => cast_unsigned_int_as_signed_or_unsigned_real_fn_meta(),
                (false, false) => cast_signed_int_as_signed_real_fn_meta(),
                (false, true) => cast_signed_int_as_unsigned_real_fn_meta(),
            }
        }
        (EvalType::Real, EvalType::Real) => {
            if !to_field_type.is_unsigned() {
                cast_real_as_signed_real_fn_meta()
            } else {
                cast_real_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Real) => {
            if !from_field_type.is_unsigned() {
                cast_string_as_signed_real_fn_meta()
            } else {
                cast_string_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Real) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Decimal, Real>()
            } else {
                cast_decimal_as_unsigned_real_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Real) => cast_any_as_any_fn_meta::<DateTime, Real>(),
        (EvalType::Duration, EvalType::Real) => cast_any_as_any_fn_meta::<Duration, Real>(),
        (EvalType::Json, EvalType::Real) => cast_any_as_any_fn_meta::<Json, Real>(),

        // any as decimal
        (EvalType::Int, EvalType::Decimal) => {
            let fu = from_field_type.is_unsigned();
            let ru = to_field_type.is_unsigned();
            match (fu, ru) {
                (true, _) => cast_unsigned_int_as_decimal_fn_meta(),
                (false, true) => cast_signed_int_as_unsigned_decimal_fn_meta(),
                (false, false) => cast_any_as_decimal_fn_meta::<Int>(),
            }
        }
        (EvalType::Real, EvalType::Decimal) => cast_real_as_decimal_fn_meta(),
        (EvalType::Bytes, EvalType::Decimal) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_decimal_fn_meta::<Bytes>()
            } else {
                cast_string_as_unsigned_decimal_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Decimal) => {
            if !to_field_type.is_unsigned() {
                cast_decimal_as_signed_decimal_fn_meta()
            } else {
                cast_decimal_as_unsigned_decimal_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Json>(),

        // any as int
        (EvalType::Int, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_int_as_int_fn_meta()
            } else {
                cast_int_as_uint_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Real, Int>()
            } else {
                cast_real_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => cast_string_as_int_or_uint_fn_meta(),
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_decimal_fn_meta::<Int>()
            } else {
                cast_decimal_as_uint_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Int) => cast_any_as_any_fn_meta::<DateTime, Int>(),
        (EvalType::Duration, EvalType::Int) => cast_any_as_any_fn_meta::<Duration, Int>(),
        (EvalType::Json, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Json, Int>()
            } else {
                cast_json_as_uint_fn_meta()
            }
        }

        // any as string
        (EvalType::Int, EvalType::Bytes) => {
            if !from_field_type.is_unsigned() {
                cast_int_as_string_fn_meta()
            } else {
                cast_uint_as_string_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Bytes) => {
            if from_field_type.tp() == FieldTypeTp::Float {
                cast_float_real_as_string_fn_meta()
            } else {
                cast_double_real_as_string_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Bytes) => cast_string_as_string_fn_meta(),
        (EvalType::Decimal, EvalType::Bytes) => cast_decimal_as_string_fn_meta(),
        (EvalType::DateTime, EvalType::Bytes) => cast_time_as_string_fn_meta(),
        (EvalType::Duration, EvalType::Bytes) => cast_duration_as_string_fn_meta(),
        (EvalType::Json, EvalType::Bytes) => cast_any_as_any_fn_meta::<Json, Bytes>(),

        // any as json
        (EvalType::Int, EvalType::Json) => {
            if from_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::IS_BOOLEAN)
            {
                cast_bool_as_json_fn_meta()
            } else if !from_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Int, Json>()
            } else {
                cast_uint_as_json_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Json) => cast_any_as_any_fn_meta::<Real, Json>(),
        (EvalType::Bytes, EvalType::Json) => cast_string_as_json_fn_meta(),
        (EvalType::Decimal, EvalType::Json) => cast_any_as_any_fn_meta::<Decimal, Json>(),
        (EvalType::DateTime, EvalType::Json) => cast_any_as_any_fn_meta::<DateTime, Json>(),
        (EvalType::Duration, EvalType::Json) => cast_any_as_any_fn_meta::<Duration, Json>(),
        (EvalType::Json, EvalType::Json) => cast_json_as_json_fn_meta(),

        // any as duration
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_bytes_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::DateTime, EvalType::Duration) => cast_time_as_duration_fn_meta(),
        (EvalType::Duration, EvalType::Duration) => cast_duration_as_duration_fn_meta(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),

        // TODO, cast any as time has not impl

        // others
        _ => return Err(other_err!("Unsupported cast from {} to {}", from, to)),
    };
    Ok(func_meta)
}

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn_rpn_node(
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let func_meta = get_cast_fn_rpn_meta(from_field_type, &to_field_type)?;
    // This cast function is inserted by `Coprocessor` automatically,
    // the `inUnion` flag always false in this situation. Ideally,
    // the cast function should be inserted by TiDB and pushed down
    // with all implicit arguments.
    Ok(RpnExpressionNode::FnCall {
        func_meta,
        args_len: 1,
        field_type: to_field_type,
        implicit_args: Vec::new(),
    })
}

/// Gets the RPN function meta
pub fn map_cast_func(expr: &Expr) -> Result<RpnFnMeta> {
    let children = expr.get_children();
    if children.len() != 1 {
        return Err(other_err!(
            "Unexpected arguments: sig {:?} with {} args",
            expr.get_sig(),
            children.len()
        ));
    }
    get_cast_fn_rpn_meta(children[0].get_field_type(), expr.get_field_type())
}

/// Indicates whether the current expression is evaluated in union statement
///
/// Note: The TiDB will push down the `inUnion` flag by implicit constant arguments,
/// but some CAST expressions inserted by TiKV coprocessor use an empty vector to represent
/// the `inUnion` flag is false.
/// See: https://github.com/pingcap/tidb/blob/1e403873d905b2d0ad3be06bd8cd261203d84638/expression/builtin.go#L260
fn in_union(implicit_args: &[ScalarValue]) -> bool {
    implicit_args.get(0) == Some(&ScalarValue::Int(Some(1)))
}

// cast any as decimal, some cast functions reuse `cast_any_as_decimal`
//
// - cast_signed_int_as_signed_decimal -> cast_any_as_decimal<Int>
// - cast_string_as_signed_decimal -> cast_any_as_decimal<Bytes>
// - cast_time_as_decimal -> cast_any_as_decimal<Time>
// - cast_duration_as_decimal -> cast_any_as_decimal<Duration>
// - cast_json_as_decimal -> cast_any_as_decimal<Json>

// because uint's upper bound is smaller than signed decimal's upper bound
// so we can merge cast uint as signed/unsigned decimal in this function
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_unsigned_int_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = Decimal::from(*val as u64);
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_signed_int_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec = if in_union(extra.implicit_args) && *val < 0 {
                Decimal::zero()
            } else {
                // FIXME, here TiDB has bug, fix this after fix TiDB's
                // if val is >=0, then val as u64 is ok,
                // if val <0, there may be bug here.
                Decimal::from(*val as u64)
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

// FIXME, here TiDB may has bug, fix this after fix TiDB's
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_real_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Real>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            let res = if in_union(extra.implicit_args) && val < 0f64 {
                Decimal::zero()
            } else {
                Decimal::from_f64(val)?
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                res,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // FIXME, in TiDB, if the param IsBinaryLiteral, then return the result of `evalDecimal` directly
            let d: Decimal = val.convert(ctx)?;
            let d = if in_union(extra.implicit_args) && d.is_negative() {
                Decimal::zero()
            } else {
                d
            };
            // FIXME, how to make a negative decimal value to unsigned decimal value
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                d,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_signed_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.clone();
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                val,
                extra.ret_field_type,
            )?))
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_unsigned_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let res = if in_union(extra.implicit_args) && val.is_negative() {
                Decimal::zero()
            } else {
                // FIXME, here TiDB may has bug, fix this after fix TiDB's
                // FIXME, how to make a unsigned decimal from negative decimal
                val.clone()
            };
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                res,
                extra.ret_field_type,
            )?))
        }
    }
}

// FIXME, for cast_int_as_decimal, TiDB's impl has bug, fix this after fixed TiDB's
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_any_as_decimal<From: Evaluable + ConvertTo<Decimal>>(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<From>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dec: Decimal = val.convert(ctx)?;
            Ok(Some(produce_dec_with_specified_tp(
                ctx,
                dec,
                extra.ret_field_type,
            )?))
        }
    }
}

// cast any as int/uint, some cast functions reuse `cast_any_as_any`
//
// - cast_real_as_int -> cast_any_as_any<Real, Int>
// - cast_decimal_as_int -> cast_any_as_any<Decimal, Int>
// - cast_time_as_int_or_uint -> cast_any_as_any<Time, Int>
// - cast_duration_as_int_or_uint -> cast_any_as_any<Duration, Int>
// - cast_json_as_int -> cast_any_as_any<Json, Int>

// this include cast_signed_or_unsigned_int_to_int
#[rpn_fn]
#[inline]
fn cast_int_as_int(val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(*val)),
    }
}

// this include cast_signed_or_unsigned_int_to_uint,
// only signed int to uint has special case.
#[rpn_fn(capture = [extra])]
#[inline]
fn cast_int_as_uint(extra: &RpnFnCallExtra<'_>, val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = *val;
            if in_union(extra.implicit_args) && val < 0i64 {
                Ok(Some(0))
            } else {
                Ok(Some(val))
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_real_as_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            if in_union(&extra.implicit_args) && val < 0f64 {
                Ok(Some(0))
            } else {
                // FIXME, mysql's double to unsigned is very special,
                //  it **seems** that if the float num bigger than i64::MAX,
                //  then return i64::MAX always.
                //  This may be the bug of mysql.
                //  So I don't change ours' behavior here.
                let val: u64 = val.convert(ctx)?;
                Ok(Some(val as i64))
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_int_or_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, in TiDB',s if `b.args[0].GetType().Hybrid()`,
            //  then it will return res from EvalInt() directly.
            let is_unsigned = extra.ret_field_type.is_unsigned();
            let val = get_valid_utf8_prefix(ctx, val.as_slice())?;
            let val = val.trim();
            let neg = val.starts_with('-');
            if !neg {
                let r: crate::codec::error::Result<u64> = val.as_bytes().convert(ctx);
                match r {
                    Ok(x) => {
                        if !is_unsigned && x > std::i64::MAX as u64 {
                            ctx.warnings
                                .append_warning(Error::cast_as_signed_overflow())
                        }
                        Ok(Some(x as i64))
                    }
                    Err(e) => handle_overflow_for_cast_string_as_int(ctx, e, false, val).map(Some),
                }
            } else if in_union(extra.implicit_args) && is_unsigned {
                Ok(Some(0))
            } else {
                let r: crate::codec::error::Result<i64> = val.as_bytes().convert(ctx);
                match r {
                    Ok(x) => {
                        if is_unsigned {
                            ctx.warnings
                                .append_warning(Error::cast_neg_int_as_unsigned());
                        }
                        Ok(Some(x))
                    }
                    Err(e) => handle_overflow_for_cast_string_as_int(ctx, e, true, val).map(Some),
                }
            }
        }
    }
}

// FIXME, TiDB's this func can return err and res at the same time, however, we can't,
//  so it may be some inconsistency between this func and TiDB's
fn handle_overflow_for_cast_string_as_int(
    ctx: &mut EvalContext,
    err: Error,
    is_neg: bool,
    origin_str: &str,
) -> Result<i64> {
    if ctx.cfg.flag.contains(Flag::IN_INSERT_STMT) && err.is_overflow() {
        // TODO, here our `handle_overflow_err` is not same as TiDB's HandleOverflow,
        //  the latter will return `err` if OverflowAsWarning, but we will return `truncated_wrong_val`.
        ctx.handle_overflow_err(Error::truncated_wrong_val("INTEGER", origin_str))?;
        if is_neg {
            Ok(std::i64::MIN)
        } else {
            Ok(std::u64::MAX as i64)
        }
    } else {
        Err(err.into())
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_uint(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Decimal>,
) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, here TiDB round before call `val.is_negative()`
            if in_union(extra.implicit_args) && val.is_negative() {
                Ok(Some(0))
            } else {
                let r: u64 = val.convert(ctx)?;
                Ok(Some(r as i64))
            }
        }
    }
}

#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_json_as_uint(ctx: &mut EvalContext, val: &Option<Json>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(j) => {
            let r: u64 = j.convert(ctx)?;
            Ok(Some(r as i64))
        }
    }
}

// cast any as real, some cast functions reuse `cast_any_as_any`
//
// cast_decimal_as_signed_real -> cast_any_as_any<Decimal, Real>
// cast_time_as_real -> cast_any_as_any<Time, Real>
// cast_duration_as_real -> cast_any_as_any<Duration, Real>
// cast_json_as_real -> by cast_any_as_any<Json, Real>

#[rpn_fn]
#[inline]
fn cast_signed_int_as_signed_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as f64).ok()),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_signed_int_as_unsigned_real(
    extra: &RpnFnCallExtra,
    val: &Option<Int>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && *val < 0 {
                Ok(Some(Real::zero()))
            } else {
                // FIXME, TiDB here may has bug, fix this after fix TiDB's
                Ok(Real::new(*val as u64 as f64).ok())
            }
        }
    }
}

// because we needn't to consider if uint overflow upper boundary of signed real,
// so we can merge uint to signed/unsigned real in one function
#[rpn_fn]
#[inline]
fn cast_unsigned_int_as_signed_or_unsigned_real(val: &Option<Int>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Real::new(*val as u64 as f64).ok()),
    }
}

#[rpn_fn]
#[inline]
fn cast_real_as_signed_real(val: &Option<Real>) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(*val)),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_real_as_unsigned_real(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Real>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && val.into_inner() < 0f64 {
                Ok(Some(Real::zero()))
            } else {
                Ok(Some(*val))
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_signed_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // FIXME, in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
            //  then return evalReal directly
            let r: f64 = val.convert(ctx)?;
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_unsigned_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // FIXME, in TiDB's builtinCastStringAsRealSig, if val is IsBinaryLiteral,
            //  then return evalReal directly
            let mut r: f64 = val.convert(ctx)?;
            if in_union(extra.implicit_args) && r < 0f64 {
                r = 0f64;
            }
            let r = produce_float_with_specified_tp(ctx, extra.ret_field_type, r)?;
            Ok(Real::new(r).ok())
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_unsigned_real(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Decimal>,
) -> Result<Option<Real>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if in_union(extra.implicit_args) && val.is_negative() {
                Ok(Some(Real::zero()))
            } else {
                // FIXME, here TiDB's may has bug, fix this after fix TiDB's
                Ok(Some(val.convert(ctx)?))
            }
        }
    }
}

// cast any as string, some cast functions reuse `cast_any_as_any`
//
// cast_json_as_string -> by cast_any_as_any<Json, String>

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_int_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Int>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = (*val).to_string().into_bytes();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_uint_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = (*val as u64).to_string().into_bytes();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_float_real_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Real>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner() as f32;
            let val = val.to_string().into_bytes();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_double_real_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Real>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.into_inner();
            let val = val.to_string().into_bytes();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_string_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.clone();
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_decimal_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Decimal>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val: Bytes = val.convert(ctx)?;
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_time_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Time>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val: Bytes = val.convert(ctx)?;
            cast_as_string_helper(ctx, extra, val)
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_duration_as_string(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<Duration>,
) -> Result<Option<Bytes>> {
    match val {
        None => Ok(None),
        Some(dur) => {
            let s: Bytes = dur.convert(ctx)?;
            cast_as_string_helper(ctx, extra, s)
        }
    }
}

fn cast_as_string_helper(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: Vec<u8>,
) -> Result<Option<Bytes>> {
    let res = produce_str_with_specified_tp(
        ctx,
        Cow::Borrowed(val.as_slice()),
        extra.ret_field_type,
        false,
    )?;
    let mut res = match res {
        Cow::Borrowed(_) => val,
        Cow::Owned(x) => x.to_vec(),
    };
    pad_zero_for_binary_type(&mut res, extra.ret_field_type);
    Ok(Some(res))
}

// cast any as json, some cast functions reuse `cast_any_as_any`
//
// - cast_int_as_json -> cast_any_as_any<Int, Json>
// - cast_real_as_json -> cast_any_as_any<Real, Json>
// - cast_decimal_as_json -> cast_any_as_any<Decimal, Json>
// - cast_time_as_json -> cast_any_as_any<Time, Json>
// - cast_duration_as_json -> cast_any_as_any<Duration, Json>

#[rpn_fn]
#[inline]
fn cast_bool_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::Boolean(*val != 0))),
    }
}

#[rpn_fn]
#[inline]
fn cast_uint_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::U64(*val as u64))),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_string_as_json(extra: &RpnFnCallExtra<'_>, val: &Option<Bytes>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if extra
                .ret_field_type
                .flag()
                .contains(FieldTypeFlag::PARSE_TO_JSON)
            {
                let s = box_try!(String::from_utf8(val.to_owned()));
                let val: Json = s.parse()?;
                Ok(Some(val))
            } else {
                // FIXME: port `JSONBinary` from TiDB to adapt if the bytes is not a valid utf8 string
                let val = unsafe { String::from_utf8_unchecked(val.to_owned()) };
                Ok(Some(Json::String(val)))
            }
        }
    }
}

#[rpn_fn]
#[inline]
fn cast_json_as_json(val: &Option<Json>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.clone())),
    }
}

// cast any as duration, no cast functions reuse `cast_any_as_any`

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_int_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let (dur, err) =
                Duration::from_i64_without_ctx(*val, extra.ret_field_type.get_decimal() as u8);
            match err {
                // in TiDB, if there is overflow err and overflow as warning,
                // then it will return isNull==true
                Some(err) => {
                    if err.is_overflow() {
                        ctx.handle_overflow_err(err)?;
                        Ok(None)
                    } else {
                        Err(err.into())
                    }
                }
                None => {
                    if dur.is_none() {
                        Err(other_err!("Expect a not none result here, this is a bug"))
                    } else {
                        Ok(Some(dur.unwrap()))
                    }
                }
            }
        }
    }
}


#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_time_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<DateTime>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dur: Duration = val.convert(ctx)?;
            Ok(Some(dur.round_frac(extra.ret_field_type.decimal() as i8)?))
        }
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_duration_as_duration(
    extra: &RpnFnCallExtra,
    val: &Option<Duration>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.round_frac(extra.ret_field_type.decimal() as i8)?)),
    }
}

macro_rules! cast_as_duration {
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<Duration>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    let result = Duration::parse($extra, extra.ret_field_type.get_decimal() as i8);
                    match result {
                        Ok(dur) => Ok(Some(dur)),
                        Err(e) => match e.code() {
                            ERR_DATA_OUT_OF_RANGE => {
                                ctx.handle_overflow_err(e)?;
                                Ok(Some(Duration::zero()))
                            }
                            WARN_DATA_TRUNCATED => {
                                ctx.handle_truncate_err(e)?;
                                Ok(Some(Duration::zero()))
                            }
                            _ => Err(e.into()),
                        },
                    }
                }
            }
        }
    };
}

cast_as_duration!(
    Real,
    cast_real_as_duration,
    val.into_inner().to_string().as_bytes()
);
cast_as_duration!(Bytes, cast_bytes_as_duration, val);
cast_as_duration!(
    Decimal,
    cast_decimal_as_duration,
    val.to_string().as_bytes()
);
cast_as_duration!(Json, cast_json_as_duration, val.unquote()?.as_bytes());

// TODO, cast any as time has not impl

// cast any as any(others cast)
#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_any_as_any<From: ConvertTo<To> + Evaluable, To: Evaluable>(
    ctx: &mut EvalContext,
    val: &Option<From>,
) -> Result<Option<To>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.convert(ctx)?;
            Ok(Some(val))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Result;
    use crate::codec::data_type::{Decimal, Real, ScalarValue, Int, Bytes};
    use crate::codec::error::{
        ERR_DATA_OUT_OF_RANGE, ERR_TRUNCATE_WRONG_VALUE, WARN_DATA_TRUNCATED,
    };
    use crate::codec::mysql::{Duration, Json, Time};
    use crate::codec::Error;
    use crate::expr::Flag;
    use crate::expr::{EvalConfig, EvalContext};
    use crate::rpn_expr::impl_cast::*;
    use crate::rpn_expr::RpnFnCallExtra;
    use crate::codec::mysql::charset::*;
    use bitfield::fmt::Display;
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;
    use std::{f64, i64};
    use tidb_query_datatype::{FieldTypeFlag, UNSPECIFIED_LENGTH};
    use std::collections::BTreeMap;

    #[test]
    fn test_in_union() {
        use super::*;

        assert_eq!(in_union(&[]), false);
        assert_eq!(in_union(&[ScalarValue::Int(None)]), false);
        assert_eq!(in_union(&[ScalarValue::Int(Some(0))]), false);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(0)), ScalarValue::Int(Some(1))]),
            false
        );
        assert_eq!(in_union(&[ScalarValue::Int(Some(1))]), true);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(1)), ScalarValue::Int(Some(0))]),
            true
        );
    }

    fn test_none_with_ctx_and_extra<F, Input, Ret>(func: F)
        where
            F: Fn(&mut EvalContext, &RpnFnCallExtra, &Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let implicit_args = [ScalarValue::Int(Some(1))];
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
            implicit_args: &implicit_args,
        };
        let r = func(&mut ctx, &extra, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_ctx<F, Input, Ret>(func: F)
        where
            F: Fn(&mut EvalContext, &Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let r = func(&mut ctx, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_extra<F, Input, Ret>(func: F)
        where
            F: Fn(&RpnFnCallExtra, &Option<Input>) -> Result<Option<Ret>>,
    {
        let implicit_args = [ScalarValue::Int(Some(1))];
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
            implicit_args: &implicit_args,
        };
        let r = func(&extra, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_nothing<F, Input, Ret>(func: F)
        where
            F: Fn(&Option<Input>) -> Result<Option<Ret>>,
    {
        let r = func(&None).unwrap();
        assert!(r.is_none());
    }

    fn make_ctx(
        overflow_as_warning: bool,
        truncate_as_warning: bool,
        should_clip_to_zero: bool,
    ) -> EvalContext {
        let mut flag: Flag = Flag::default();
        if overflow_as_warning {
            flag |= Flag::OVERFLOW_AS_WARNING;
        }
        if truncate_as_warning {
            flag |= Flag::TRUNCATE_AS_WARNING;
        }
        if should_clip_to_zero {
            flag |= Flag::IN_INSERT_STMT;
        }
        let cfg = Arc::new(EvalConfig::from_flag(flag));
        EvalContext::new(cfg)
    }

    fn make_implicit_args(in_union: bool) -> [ScalarValue; 1] {
        if in_union {
            [ScalarValue::Int(Some(1))]
        } else {
            [ScalarValue::Int(Some(0))]
        }
    }

    fn make_ret_field_type(unsigned: bool) -> FieldType {
        let mut ft = if unsigned {
            let mut ft = FieldType::default();
            ft.as_mut_accessor().set_flag(FieldTypeFlag::UNSIGNED);
            ft
        } else {
            FieldType::default()
        };
        let fta = ft.as_mut_accessor();
        fta.set_flen(UNSPECIFIED_LENGTH);
        fta.set_decimal(UNSPECIFIED_LENGTH);
        ft
    }

    fn make_ret_field_type_2(unsigned: bool, flen: isize, decimal: isize) -> FieldType {
        let mut ft = if unsigned {
            let mut ft = FieldType::default();
            ft.as_mut_accessor().set_flag(FieldTypeFlag::UNSIGNED);
            ft
        } else {
            FieldType::default()
        };
        let fta = ft.as_mut_accessor();
        fta.set_flen(flen);
        fta.set_decimal(decimal);
        ft
    }

    fn make_extra<'a>(
        ret_field_type: &'a FieldType,
        implicit_args: &'a [ScalarValue],
    ) -> RpnFnCallExtra<'a> {
        RpnFnCallExtra {
            ret_field_type,
            implicit_args,
        }
    }

    fn check_overflow(ctx: &EvalContext, overflow: bool) {
        if overflow {
            assert_eq!(ctx.warnings.warning_cnt, 1);
            assert_eq!(ctx.warnings.warnings[0].get_code(), ERR_DATA_OUT_OF_RANGE);
        } else {
            assert_eq!(ctx.warnings.warning_cnt, 0);
        }
    }

    fn check_truncated(ctx: &EvalContext, truncated: bool) {
        if truncated {
            assert_eq!(ctx.warnings.warning_cnt, 1);
            assert_eq!(ctx.warnings.warnings[0].get_code(), WARN_DATA_TRUNCATED);
        } else {
            assert_eq!(ctx.warnings.warning_cnt, 0);
        }
    }

    fn check_result<P: ToString, R: Debug + PartialEq>(
        input: &P,
        expect: Option<&R>,
        res: &Result<Option<R>>,
    ) {
        assert!(
            res.is_ok(),
            "input: {}, expect: {:?}, output: {:?}",
            input.to_string(),
            expect,
            res
        );
        let res = res.as_ref().unwrap();
        if res.is_none() {
            assert!(
                expect.is_none(),
                "input: {}, expect: {:?}, output: {:?}",
                input.to_string(),
                expect,
                res
            );
        } else {
            let res = res.as_ref().unwrap();
            assert_eq!(
                res,
                expect.unwrap(),
                "input: {}, expect: {:?}, output: {:?}",
                input.to_string(),
                expect,
                res
            );
        }
    }

    fn truncate_str(mut s: String, len: usize) -> Vec<u8> {
        s.truncate(len);
        s.into_bytes()
    }

    impl Display for Json {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "{}", self.to_string())
        }
    }

    // comment for all test below:
    // if there should not be any overflow/truncate,
    // then should not set ctx with overflow_as_warning/truncated_as_warning flag,
    // and then if there is unexpected overflow/truncate,
    // then we will find them in `unwrap`

    #[test]
    fn test_int_as_int() {
        test_none_with_nothing(cast_int_as_int);

        let cs = vec![
            (i64::MAX, i64::MAX),
            (i64::MIN, i64::MIN),
            (u64::MAX as i64, u64::MAX as i64),
        ];
        for (input, expect) in cs {
            let r = cast_int_as_int(&Some(input));
            check_result(&input, Some(&expect), &r);
        }
    }

    #[test]
    fn test_int_as_uint() {
        test_none_with_extra(cast_int_as_uint);

        let cs = vec![
            // (origin, result, in_union)
            (-10, 0u64, true),
            (10, 10u64, true),
            (i64::MAX, i64::MAX as u64, true),
            (u64::MAX as i64, 0u64, true),
            (-10, (-10i64) as u64, false),
            (10, 10u64, false),
            (i64::MAX, i64::MAX as u64, false),
            (u64::MAX as i64, u64::MAX, false),
        ];
        for (input, expect, in_union) in cs {
            let rtf = make_ret_field_type(true);
            let ia = make_implicit_args(in_union);
            let extra = make_extra(&rtf, &ia);
            let r = cast_int_as_uint(&extra, &Some(input));
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input, Some(&expect), &r);
        }
    }

    #[test]
    fn test_real_as_int() {
        test_none_with_ctx(cast_any_as_any::<Real, Int>);

        let cs = vec![
            // (origin, result, overflow)
            (-10.4, -10i64, false),
            (-10.5, -11, false),
            (10.4, 10, false),
            (10.5, 11, false),
            (i64::MAX as f64, i64::MAX, false),
            ((1u64 << 63) as f64, i64::MAX, false),
            (i64::MIN as f64, i64::MIN, false),
            ((1u64 << 63) as f64 + (1u64 << 62) as f64, i64::MAX, true),
            ((i64::MIN as f64) * 2f64, i64::MIN, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let r = cast_any_as_any::<Real, Int>(&mut ctx, &Real::new(input).ok());
            check_result(&input, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }
    }

    #[test]
    fn test_real_as_uint() {
        test_none_with_ctx_and_extra(cast_real_as_uint);

        // in_union
        let cs = vec![
            // (input, result)
            (-10.0, 0u64),
            (i64::MIN as f64, 0),
            (10.0, 10u64),
            (i64::MAX as f64, (1u64 << 63)),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let rtf = make_ret_field_type(true);
            let ia = make_implicit_args(true);
            let extra = make_extra(&rtf, &ia);
            let r = cast_real_as_uint(&mut ctx, &extra, &Real::new(input).ok());
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input, Some(&result), &r);
        }

        // no clip to zero
        let cs = vec![
            // (origin, result, overflow)
            (10.5, 11u64, false),
            (10.4, 10u64, false),
            (
                ((1u64 << 63) + (1u64 << 62)) as f64,
                ((1u64 << 63) + (1u64 << 62)),
                false,
            ),
            (u64::MAX as f64, i64::MAX as u64, false),
            ((u64::MAX as f64) * 2f64, i64::MAX as u64, true),
            (-1f64, -1f64 as i64 as u64, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rtf = make_ret_field_type(true);
            let extra = make_extra(&rtf, &ia);
            let r = cast_real_as_uint(&mut ctx, &extra, &Real::new(input).ok());
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input, Some(&result), &r);
            check_overflow(&ctx, overflow)
        }

        // should clip to zero
        let cs: Vec<(f64, u64, bool)> = vec![
            // (origin, result, overflow)
            (-1f64, 0, true),
            (i64::MIN as f64, 0, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, true);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_real_as_uint(&mut ctx, &extra, &Real::new(input).ok());
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input, Some(&result), &r);
            check_overflow(&ctx, overflow)
        }
    }

    #[test]
    fn test_string_as_int() {
        test_none_with_ctx_and_extra(cast_string_as_int_or_uint);

        let cs = vec![
            // (origin, result, overflow)
            ("-10", -10i64, false),
            ("-10", -10i64, false),
            ("9223372036854775807", 9223372036854775807i64, false),
            ("-9223372036854775808", -9223372036854775808i64, false),
            ("9223372036854775808", 9223372036854775807i64, true),
            ("-9223372036854775809", -9223372036854775808i64, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);
            let r =
                cast_string_as_int_or_uint(&mut ctx, &extra, &Some(Vec::from(input.as_bytes())));
            check_result(&input, Some(&result), &r);
            check_overflow(&ctx, overflow)
        }
    }

    #[test]
    fn test_string_as_uint() {
        test_none_with_ctx_and_extra(cast_string_as_int_or_uint);

        let cs: Vec<(String, u64, bool)> = vec![
            // (origin, result, overflow)
            (i64::MAX.to_string(), i64::MAX as u64, false),
            (u64::MAX.to_string(), u64::MAX, false),
            (String::from("99999999999999999999999"), 0, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);

            let r =
                cast_string_as_int_or_uint(&mut ctx, &extra, &Some(Vec::from(input.as_bytes())));
            assert!(r.is_ok());
            assert_eq!(r.unwrap().unwrap() as u64, result);
            check_overflow(&ctx, overflow);
        }

        let cs: Vec<(&str, i64, i64, bool)> = vec![
            // (origin, result, warning_cnt, in_union)
            ("-10", -10, 1, false),
            ("-10", 0, 0, true),
        ];

        // TODO, warning_cnt
        for (input, result, _warning_cnt, in_union) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);

            let r =
                cast_string_as_int_or_uint(&mut ctx, &extra, &Some(Vec::from(input.as_bytes())));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_decimal_as_int() {
        test_none_with_ctx(cast_any_as_any::<Decimal, Int>);
        let cs: Vec<(Decimal, i64, bool)> = vec![
            // (origin, result, overflow)
            (
                Decimal::from_bytes("9223372036854775807".as_bytes())
                    .unwrap()
                    .unwrap(),
                9223372036854775807,
                false,
            ),
            (
                Decimal::from_bytes("-9223372036854775808".as_bytes())
                    .unwrap()
                    .unwrap(),
                -9223372036854775808,
                false,
            ),
            (
                Decimal::from_bytes("9223372036854775808".as_bytes())
                    .unwrap()
                    .unwrap(),
                9223372036854775807,
                true,
            ),
            (
                Decimal::from_bytes("-9223372036854775809".as_bytes())
                    .unwrap()
                    .unwrap(),
                -9223372036854775808,
                true,
            ),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let input_copy = input.clone();
            let r = cast_any_as_any::<Decimal, Int>(&mut ctx, &Some(input));
            check_result(&input_copy, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }
    }

    // TODO
    #[test]
    fn test_decimal_as_uint() {
        test_none_with_ctx_and_extra(cast_decimal_as_uint);
        // in_union
        let cs: Vec<(Decimal, u64)> = vec![
            (
                Decimal::from_bytes("-9223372036854775808".as_bytes())
                    .unwrap()
                    .unwrap(),
                0,
            ),
            (
                Decimal::from_bytes("-9223372036854775809".as_bytes())
                    .unwrap()
                    .unwrap(),
                0,
            ),
            (
                Decimal::from_bytes("9223372036854775808".as_bytes())
                    .unwrap()
                    .unwrap(),
                9223372036854775808,
            ),
            (
                Decimal::from_bytes("18446744073709551615".as_bytes())
                    .unwrap()
                    .unwrap(),
                18446744073709551615,
            ),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(true);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);

            let input_copy = input.clone();
            let r = cast_decimal_as_uint(&mut ctx, &extra, &Some(input));
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input_copy, Some(&result), &r);
        }

        let cs: Vec<(Decimal, u64, bool)> = vec![
            // (input, result, overflow)
            (
                Decimal::from_bytes("10".as_bytes()).unwrap().unwrap(),
                10,
                false,
            ),
            (
                Decimal::from_bytes("1844674407370955161".as_bytes())
                    .unwrap()
                    .unwrap(),
                1844674407370955161,
                false,
            ),
            (
                Decimal::from_bytes("-10".as_bytes()).unwrap().unwrap(),
                0,
                true,
            ),
            (
                Decimal::from_bytes("18446744073709551616".as_bytes())
                    .unwrap()
                    .unwrap(),
                u64::MAX,
                true,
            ),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);

            let input_copy = input.clone();
            let r = cast_decimal_as_uint(&mut ctx, &extra, &Some(input));
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input_copy, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }
    }

    #[test]
    fn test_time_as_int_and_uint() {
        // TODO, add more test case
        // TODO, add test that make cast_any_as_any::<Time, Int> returning truncated error
        let cs: Vec<(Time, i64)> = vec![
            (Time::parse_utc_datetime("11:11:11", 0).unwrap(), 111111),
            (
                Time::parse_utc_datetime("11:11:11.6666", 4).unwrap(),
                111112,
            ),
        ];

        for (input, result) in cs {
            let mut ctx = EvalContext::default();
            let input_copy = input.clone();
            let r = cast_any_as_any::<Time, Int>(&mut ctx, &Some(input));
            check_result(&input_copy, Some(&result), &r);
        }
    }

    #[test]
    fn test_duration_as_int() {
        // TODO, add more test case
        let cs: Vec<(Duration, i64)> = vec![
            (
                Duration::parse("17:51:04.78".as_bytes(), 2).unwrap(),
                175105,
            ),
            (
                Duration::parse("-17:51:04.78".as_bytes(), 2).unwrap(),
                -175105,
            ),
            (
                Duration::parse("17:51:04.78".as_bytes(), 0).unwrap(),
                175104,
            ),
            (
                Duration::parse("-17:51:04.78".as_bytes(), 0).unwrap(),
                -175104,
            ),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(true, false, false);
            let r = cast_any_as_any::<Duration, Int>(&mut ctx, &Some(input));
            check_result(&input, Some(&result), &r);
        }
    }

    // TODO
    #[test]
    fn test_json_as_int() {
        test_none_with_ctx(cast_any_as_any::<Json, Int>);

        // no overflow
        let cs = vec![
            // (origin, result, overflow)
            (Json::Object(BTreeMap::default()), 0, false),
            (Json::Array(vec![]), 0, false),
            (Json::I64(10), 10i64, false),
            (Json::I64(i64::MAX), i64::MAX, false),
            (Json::I64(i64::MIN), i64::MIN, false),
            (Json::U64(0), 0, false),
            (Json::U64(u64::MAX), u64::MAX as i64, false),
            (Json::Double(i64::MIN as u64 as f64), i64::MAX, false),
            (Json::Double(i64::MAX as u64 as f64), i64::MAX, false),
            (Json::Double(i64::MIN as u64 as f64), i64::MAX, false),
            (Json::Double(i64::MIN as f64), i64::MIN, false),
            (Json::Double(10.5), 11, false),
            (Json::Double(10.4), 10, false),
            (Json::Double(-10.4), -10, false),
            (Json::Double(-10.5), -11, false),
            (Json::String(String::from("10.0")), 10, false),
            (Json::Boolean(true), 1, false),
            (Json::Boolean(false), 0, false),
            (Json::None, 0, false),
            (
                Json::Double(((1u64 << 63) + (1u64 << 62)) as u64 as f64),
                i64::MAX,
                true,
            ),
            (
                Json::Double(((1u64 << 63) + (1u64 << 62)) as f64),
                i64::MIN,
                true,
            ),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let input_copy = input.clone();
            let r = cast_any_as_any::<Json, Int>(&mut ctx, &Some(input));
            check_result(&input_copy, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }
    }

    #[test]
    fn test_json_as_uint() {
        // no clip to zero
        let cs = vec![
            // (origin, result, overflow)
            (Json::Double(-1.0), -1.0f64 as i64 as u64, false),
            (Json::String(String::from("10")), 10, false),
            (Json::String(String::from("+10abc")), 10, false),
            (
                Json::String(String::from("9999999999999999999999999")),
                u64::MAX,
                true,
            ),
            (Json::Double(2f64 * (u64::MAX as f64)), u64::MAX, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let input_copy = input.clone();
            let r = cast_json_as_uint(&mut ctx, &Some(input));
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input_copy, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }

        // should clip to zero
        let cs = vec![
            // (origin, result, overflow)
            (Json::Double(-1.0), 0, false),
            (Json::String(String::from("-10")), 0, false),
            (Json::String(String::from("10")), 10, false),
            (Json::String(String::from("+10abc")), 10, false),
            (
                Json::String(String::from("9999999999999999999999999")),
                u64::MAX,
                true,
            ),
            (Json::Double(2f64 * (u64::MAX as f64)), u64::MAX, true),
        ];

        for (input, result, overflow) in cs {
            let mut ctx = make_ctx(true, false, true);
            let input_copy = input.clone();
            let r = cast_json_as_uint(&mut ctx, &Some(input));
            let r = r.map(|x| x.map(|x| x as u64));
            check_result(&input_copy, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }
    }

    #[test]
    fn tes_signed_int_as_signed_real() {
        test_none_with_nothing(cast_signed_int_as_signed_real);

        let cs: Vec<(i64, f64)> = vec![
            // (input, result)
            (i64::MIN, i64::MIN as f64),
            (0, 0f64),
            (i64::MAX, i64::MAX as f64),
        ];

        for (input, result) in cs {
            let r = cast_signed_int_as_signed_real(&Some(input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_signed_int_as_unsigned_real() {
        test_none_with_extra(cast_signed_int_as_unsigned_real);

        let cs: Vec<(i64, f64, bool)> = vec![
            // (input, result, in_union)
            // not in union
            // TODO, add test case of negative int to unsigned real
            // (i64::MIN, i64::MIN as u64 as f64, false),
            (i64::MAX, i64::MAX as f64, false),
            (0, 0f64, false),
            // in union
            (i64::MIN, 0f64, true),
            (-1, -1f64, true),
            (i64::MAX, i64::MAX as f64, true),
            (0, 0f64, true),
        ];
        for (input, result, in_union) in cs {
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_signed_int_as_unsigned_real(&extra, &Some(input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_unsigned_int_as_signed_or_unsigned_real() {
        test_none_with_nothing(cast_unsigned_int_as_signed_or_unsigned_real);

        let cs = vec![
            // (input, result)
            (0, 0f64),
            (u64::MAX, u64::MAX as f64),
            (i64::MAX as u64, i64::MAX as u64 as f64),
        ];
        for (input, result) in cs {
            let r = cast_unsigned_int_as_signed_or_unsigned_real(&Some(input as i64));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_real_as_signed_real() {
        test_none_with_nothing(cast_real_as_signed_real);

        let cs = vec![
            // (input, result)
            (f32::MIN as f64, f32::MIN as f64),
            (f32::MAX as f64, f32::MAX as f64),
            (f64::MIN, f64::MIN),
            (0f64, 0f64),
            (f64::MAX, f64::MAX),
            (i64::MIN as f64, i64::MIN as f64),
            (i64::MAX as f64, i64::MAX as f64),
            (u64::MAX as f64, u64::MAX as f64),
        ];
        for (input, result) in cs {
            let r = cast_real_as_signed_real(&Real::new(input).ok());
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_real_as_unsigned_real() {
        let cs = vec![
            // (input, result, in_union)
            // not in union
            // TODO, add test case of negative real to unsigned real
            // (-1.0, -1.0, false),
            // (i64::MIN as f64, i64::MIN as f64, false),
            // (f64::MIN, f64::MIN, false),
            (u64::MIN as f64, u64::MIN as f64, false),
            (1.0, 1.0, false),
            (i64::MAX as f64, i64::MAX as f64, false),
            (u64::MAX as f64, u64::MAX as f64, false),
            (f64::MAX, f64::MAX, false),
            // in union
            (-1.0, 0.0, true),
            (i64::MIN as f64, 0.0, true),
            (u64::MIN as f64, 0.0, true),
            (f64::MIN, 0.0, true),
            (1.0, 1.0, true),
            (i64::MAX as f64, i64::MAX as f64, true),
            (u64::MAX as f64, u64::MAX as f64, true),
            (f64::MAX, f64::MAX, true),
        ];

        for (input, result, in_union) in cs {
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_real_as_unsigned_real(&extra, &Real::new(input).ok());
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_string_as_signed_real() {
        test_none_with_ctx_and_extra(cast_string_as_signed_real);

        let ul = UNSPECIFIED_LENGTH;
        let cs: Vec<(String, f64, isize, isize, bool, bool)> = vec![
            // (input, result, flen, decimal, truncated, overflow)
            // no special flen and decimal
            (String::from("99999999"), 99999999f64, ul, ul, false, false),
            (String::from("1234abc"), 1234f64, ul, ul, true, false),
            (String::from("-1234abc"), -1234f64, ul, ul, true, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
            ),
            (
                (0..401)
                    .map(|x| if x == 0 { '-' } else { '9' })
                    .collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
            ),
            // with special flen and decimal
            (String::from("99999999"), 99999999f64, 8, 0, false, false),
            (String::from("99999999"), 99999999f64, 9, 0, false, false),
            (String::from("99999999"), 9999999f64, 7, 0, false, true),
            (String::from("99999999"), 999999f64, 8, 2, false, true),
            (String::from("1234abc"), 0.9f64, 1, 1, false, true),
            (String::from("-1234abc"), -0.9f64, 1, 1, false, true),
        ];

        for (input, result, flen, decimal, truncated, overflow) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, flen, decimal);
            let extra = make_extra(&rft, &ia);
            let r = cast_string_as_signed_real(&mut ctx, &extra, &Some(input.clone().into_bytes()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
            check_truncated(&ctx, truncated);
            check_overflow(&ctx, overflow);
        }
    }

    #[test]
    fn test_string_as_unsigned_real() {
        test_none_with_ctx_and_extra(cast_string_as_unsigned_real);

        let ul = UNSPECIFIED_LENGTH;
        let cs: Vec<(String, f64, isize, isize, bool, bool, bool)> = vec![
            // (input, result, flen, decimal, truncated, overflow, in_union)

            // not in union
            (
                String::from("99999999"),
                99999999f64,
                ul,
                ul,
                false,
                false,
                false,
            ),
            (String::from("1234abc"), 1234f64, ul, ul, true, false, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
                false,
            ),
            // TODO, add test case for negative float to unsigned float
            // (String::from("-1234abc"), -1234f64, ul, ul, true, false, false),
            // (
            //     (0..401)
            //         .map(|x| if x == 0 { '-' } else { '9' })
            //         .collect::<String>(),
            //     f64::MAX, ul, ul, true, false, false,
            // ),
            // (String::from("-1234abc"), -1234.0, 4, 0, true, false, false),
            // (String::from("-1234abc"), -999.9, 4, 1, true, true, false),
            // (String::from("-1234abc"), -99.99, 4, 2, true, true, false),
            // (String::from("-1234abc"), -99.9, 3, 1, true, true, false),
            // (String::from("-1234abc"), -9.999, 4, 3, true, true, false),
            (
                String::from("99999999"),
                99999999f64,
                8,
                0,
                false,
                false,
                false,
            ),
            (
                String::from("99999999"),
                9999999.9,
                8,
                1,
                false,
                true,
                false,
            ),
            (
                String::from("99999999"),
                999999.99,
                8,
                2,
                false,
                true,
                false,
            ),
            (String::from("99999999"), 999999.9, 7, 1, false, true, false),
            (String::from("1234abc"), 1234.0, 4, 0, true, false, false),
            (String::from("1234abc"), 999.9, 4, 1, true, true, false),
            (String::from("1234abc"), 99.99, 4, 2, true, true, false),
            (String::from("1234abc"), 99.9, 3, 1, true, true, false),
            (String::from("1234abc"), 9.999, 4, 3, true, true, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                9999999999.0,
                10,
                0,
                true,
                true,
                false,
            ),
            (
                (0..400).map(|_| '9').collect::<String>(),
                999999999.9,
                10,
                1,
                true,
                true,
                false,
            ),
            // (
            //     (0..401)
            //         .map(|x| if x == 0 { '-' } else { '9' })
            //         .collect::<String>(),
            //     f64::MAX, ul, ul, true, false, false,
            // ),

            // in union
            // in union and neg
            (String::from("-190"), 0f64, ul, ul, false, false, true),
            (String::from("-10abc"), 0f64, ul, ul, true, false, true),
            (
                String::from("-1234abc"),
                -1234f64,
                ul,
                ul,
                true,
                false,
                true,
            ),
            (
                String::from("-1234abc"),
                -1234f64,
                ul,
                ul,
                true,
                false,
                false,
            ),
            (
                (0..401)
                    .map(|x| if x == 0 { '-' } else { '9' })
                    .collect::<String>(),
                f64::MAX,
                ul,
                ul,
                true,
                false,
                false,
            ),
            (String::from("-1234abc"), 0.0, 4, 0, true, false, false),
            (String::from("-1234abc"), 0.0, 4, 1, true, false, false),
            (String::from("-1234abc"), 0.0, 4, 2, true, false, false),
            (String::from("-1234abc"), 0.0, 3, 1, true, false, false),
            (String::from("-1234abc"), 0.0, 4, 3, true, false, false),
            (
                (0..401)
                    .map(|x| if x == 0 { '-' } else { '9' })
                    .collect::<String>(),
                0.0,
                ul,
                ul,
                true,
                false,
                false,
            ),
            // in union but not neg, so same as not in union
            (
                String::from("99999999"),
                99999999f64,
                8,
                0,
                false,
                false,
                false,
            ),
            (
                String::from("99999999"),
                9999999.9,
                8,
                1,
                false,
                true,
                false,
            ),
            (
                String::from("99999999"),
                999999.99,
                8,
                2,
                false,
                true,
                false,
            ),
            (String::from("99999999"), 999999.9, 7, 1, false, true, false),
            (String::from("1234abc"), 1234.0, 4, 0, true, false, false),
            (String::from("1234abc"), 999.9, 4, 1, true, true, false),
            (String::from("1234abc"), 99.99, 4, 2, true, true, false),
            (String::from("1234abc"), 99.9, 3, 1, true, true, false),
            (String::from("1234abc"), 9.999, 4, 3, true, true, false),
            (
                (0..400).map(|_| '9').collect::<String>(),
                9999999999.0,
                10,
                0,
                true,
                true,
                false,
            ),
            (
                (0..400).map(|_| '9').collect::<String>(),
                999999999.9,
                10,
                1,
                true,
                true,
                false,
            ),
        ];

        for (input, result, flen, decimal, truncated, overflow, in_union) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type_2(true, flen, decimal);
            let extra = make_extra(&rft, &ia);
            let p = Some(input.clone().into_bytes());
            let r = cast_string_as_unsigned_real(&mut ctx, &extra, &p);
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
            check_truncated(&ctx, truncated);
            check_overflow(&ctx, overflow)
        }
    }

    #[test]
    fn test_decimal_as_signed_real() {
        test_none_with_ctx(cast_any_as_any::<Decimal, Int>);

        // because decimal can always be represent by signed real,
        // so we needn't to check whether get truncated err.
        let cs = vec![
            // (input, result)
            (Decimal::from_f64(-10.0).unwrap(), -10.0),
            (Decimal::from_f64(i64::MIN as f64).unwrap(), i64::MIN as f64),
            (Decimal::from_f64(i64::MAX as f64).unwrap(), i64::MAX as f64),
            (Decimal::from_f64(u64::MAX as f64).unwrap(), u64::MAX as f64),
        ];
        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Decimal, Real>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_decimal_as_unsigned_real() {
        test_none_with_ctx_and_extra(cast_decimal_as_unsigned_real);

        let cs: Vec<(Decimal, f64, bool, bool)> = vec![
            // (origin, result, in_union, overflow)
            // not in union
            (Decimal::from(0), 0.0, false, false),
            (
                Decimal::from(9223372036854775807u64),
                9223372036854775807.0,
                false,
                false,
            ),
            (
                Decimal::from_bytes("9223372036854775809".as_bytes())
                    .unwrap()
                    .unwrap(),
                9223372036854775809.0,
                false,
                false,
            ),
            // TODO, add test case for negative decimal to unsigned real

            // in union
            (Decimal::from(-1023), 0f64, true, false),
            (Decimal::from(-10), 0f64, true, false),
            (Decimal::from(i64::MIN), 0f64, true, false),
            (Decimal::from(1023), 1023.0, true, false),
            (Decimal::from(10), 10.0, true, false),
            (Decimal::from(i64::MAX), i64::MAX as f64, true, false),
            (Decimal::from(u64::MAX), u64::MAX as f64, true, false),
            (
                Decimal::from(1844674407370955161u64),
                1844674407370955161u64 as f64,
                true,
                false,
            ),
            (
                Decimal::from_bytes("18446744073709551616".as_bytes())
                    .unwrap()
                    .unwrap(),
                // 18446744073709551616 - u64::MAX==1,
                // but u64::MAX as f64 == 18446744073709551616
                u64::MAX as f64,
                true,
                false,
            ),
        ];

        for (input, result, in_union, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(in_union);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_decimal_as_unsigned_real(&mut ctx, &extra, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
            check_overflow(&ctx, overflow);
        }
    }

    #[test]
    fn test_time_as_real() {
        test_none_with_ctx(cast_any_as_any::<Time, Real>);

        // TODO, add more test case
        let cs = vec![
            // (input, result)
            (Time::parse_utc_datetime("11:11:11", 0).unwrap(), 111111.0),
            (
                Time::parse_utc_datetime("11:11:11.6666", 4).unwrap(),
                111111.6666,
            ),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Time, Real>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_duration_as_real() {
        // TODO, add more test case
        let cs = vec![
            // (input, result)
            (
                Duration::parse("17:51:04.78".as_bytes(), 2).unwrap(),
                175104.78,
            ),
            (
                Duration::parse("-17:51:04.78".as_bytes(), 2).unwrap(),
                -175104.78,
            ),
            (
                Duration::parse("17:51:04.78".as_bytes(), 0).unwrap(),
                175104.0,
            ),
            (
                Duration::parse("-17:51:04.78".as_bytes(), 0).unwrap(),
                -175104.0,
            ),
        ];
        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Duration, Real>(&mut ctx, &Some(input));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_json_as_real() {
        let cs: Vec<(Json, f64, bool)> = vec![
            // (input, result, truncated)
            (Json::Object(BTreeMap::default()), 0f64, false),
            (Json::Array(vec![]), 0f64, false),
            (Json::I64(10), 10f64, false),
            (Json::I64(i64::MAX), i64::MAX as f64, false),
            (Json::I64(i64::MIN), i64::MIN as f64, false),
            (Json::U64(0), 0f64, false),
            (Json::U64(u64::MAX), u64::MAX as f64, false),
            (Json::Double(f64::MAX), f64::MAX, false),
            (Json::Double(f64::MIN), f64::MIN, false),
            (Json::String(String::from("10.0")), 10.0, false),
            (Json::String(String::from("-10.0")), -10.0, false),
            (Json::Boolean(true), 1f64, false),
            (Json::Boolean(false), 0f64, false),
            (Json::None, 0f64, false),
            (
                Json::String((0..500).map(|_| '9').collect::<String>()),
                f64::MAX,
                true,
            ),
            (
                Json::String(
                    (0..500)
                        .map(|x| if x == 0 { '-' } else { '9' })
                        .collect::<String>(),
                ),
                f64::MIN,
                true,
            ),
        ];

        for (input, result, truncated) in cs {
            let mut ctx = make_ctx(false, true, false);
            let r = cast_any_as_any::<Json, Real>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| x.into_inner()));
            check_result(&input, Some(&result), &r);
            check_truncated(&ctx, truncated);
        }
    }

    #[test]
    fn test_int_as_string() {
        test_none_with_ctx_and_extra(cast_int_as_string);

        let cs: Vec<(i64, Vec<u8>, isize, &str)> = vec![
            // (input, result, flen, charset)

            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_UTF8),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_UTF8MB4),
            (i64::MIN, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_UTF8MB4),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_ASCII),
            (i64::MIN, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_ASCII),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_LATIN1),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_BIN),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_UTF8),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_UTF8MB4),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_ASCII),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_LATIN1),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_BIN),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_UTF8),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_UTF8MB4),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_ASCII),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_LATIN1),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_BIN),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_UTF8),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_UTF8MB4),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_UTF8MB4),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_ASCII),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_ASCII),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_LATIN1),
            (i64::MIN, truncate_str(i64::MIN.to_string(), 18), 18, CHARSET_BIN),
            (i64::MIN, i64::MIN.to_string(), 19, CHARSET_UTF8),
            (i64::MIN, i64::MIN.to_string(), 19, CHARSET_UTF8MB4),
            (i64::MIN, i64::MIN.to_string(), 19, CHARSET_ASCII),
            (i64::MIN, i64::MIN.to_string(), 19, CHARSET_LATIN1),
            (i64::MIN, i64::MIN.to_string(), 19, CHARSET_BIN),
            (i64::MIN, i64::MIN.to_string(), 30, CHARSET_UTF8),
            (i64::MIN, i64::MIN.to_string(), 30, CHARSET_UTF8MB4),
            (i64::MIN, i64::MIN.to_string(), 30, CHARSET_ASCII),
            (i64::MIN, i64::MIN.to_string(), 30, CHARSET_LATIN1),
            (i64::MIN, i64::MIN.to_string(), 30, CHARSET_BIN),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(unsigned);
            let extra = make_extra(&rft, &ia);
            let r = cast_int_as_string(&mut ctx, &extra, &Some(input));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_uint_as_string() {
        test_none_with_ctx_and_extra(cast_uint_as_string);

        let cs: Vec<(u64, Vec<u8>, isize, &str)> = vec![
            // (input, result, flen, charset)

            // unsigned
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_UTF8),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_UTF8MB4),
            (i64::MIN, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_UTF8MB4),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_ASCII),
            (i64::MIN, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_ASCII),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_LATIN1),
            (i64::MAX, truncate_str(i64::MAX.to_string(), 18), 18, CHARSET_BIN),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_UTF8),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_UTF8MB4),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_ASCII),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_LATIN1),
            (i64::MAX, i64::MAX.to_string(), 19, CHARSET_BIN),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_UTF8),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_UTF8MB4),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_ASCII),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_LATIN1),
            (i64::MAX, i64::MAX.to_string(), 30, CHARSET_BIN),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_UTF8),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_UTF8MB4),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_UTF8MB4),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_ASCII),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_ASCII),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_LATIN1),
            (u64::MIN, truncate_str(u64::MIN.to_string(), 18), 18, CHARSET_BIN),
            (u64::MIN, u64::MIN.to_string(), 19, CHARSET_UTF8),
            (u64::MIN, u64::MIN.to_string(), 19, CHARSET_UTF8MB4),
            (u64::MIN, u64::MIN.to_string(), 19, CHARSET_ASCII),
            (u64::MIN, u64::MIN.to_string(), 19, CHARSET_LATIN1),
            (u64::MIN, u64::MIN.to_string(), 19, CHARSET_BIN),
            (u64::MIN, u64::MIN.to_string(), 30, CHARSET_UTF8),
            (u64::MIN, u64::MIN.to_string(), 30, CHARSET_UTF8MB4),
            (u64::MIN, u64::MIN.to_string(), 30, CHARSET_ASCII),
            (u64::MIN, u64::MIN.to_string(), 30, CHARSET_LATIN1),
            (u64::MIN, u64::MIN.to_string(), 30, CHARSET_BIN),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(true);
            let extra = make_extra(&rft, &ia);
            let r = cast_uint_as_string(&mut ctx, &extra, &Some(input));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_real_as_string() {
        let cs: Vec<(f64, String, bool)> = vec![
            // (input, result, is_f64)
            // f32
            (f32::MIN as f64, f32::MIN.to_string(), false),
            (f32::MAX as f64, f32::MAX.to_string(), false),

            // f64
            (f64::MIN, f64::MIN.to_string(), true),
            (0.0, 0.0.to_string(), true),
            (f64::MAX, f64::MAX.to_string(), true),

            // TODO, add test case for ProduceStrWithSpecifiedTp
        ];

        for (input, result, is_f64) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);
            let r = if is_f64 {
                cast_double_real_as_string(&mut ctx, &extra, &Real::new(input).ok())
            } else {
                cast_float_real_as_string(&mut ctx, &extra, &Real::new(input as f32 as f64).ok())
            };
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_string_as_string() {
        let cs = vec![
            ()
        ];
    }

    #[test]
    fn test_decimal_as_string() {
        test_none_with_ctx_and_extra(cast_decimal_as_string);

        let cs: Vec<(Decimal, String)> = vec![
            // (input, result)
            (Decimal::from(99999), "99999".to_string()),
            (Decimal::from(-99999), "-99999".to_string()),
            (Decimal::from_f64(10.240).unwrap(), "10.240".to_string()),
            (Decimal::from_f64(f64::MIN).unwrap(), f64::MIN.to_string()),
            (Decimal::from_f64(f64::MAX).unwrap(), f64::MAX.to_string()),
            (Decimal::from_f64(f32::MIN as f64).unwrap(), f32::MIN.to_string()),
            (Decimal::from_f64(f32::MAX as f64).unwrap(), f32::MAX.to_string()),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);
            let r = cast_decimal_as_string(&mut ctx, &extra, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_time_as_string() {
        test_none_with_ctx_and_extra(cast_time_as_string);

        // TODO add more test case
        let cs: Vec<(Time, String)> = vec![
            // (input, result)
            (
                Time::parse_utc_datetime("11:11:11", 0).unwrap(),
                "11:11:11".to_string()
            ),
            (
                Time::parse_utc_datetime("11:11:11.6666", 4).unwrap(),
                "11:11:11.6666".to_string()
            ),

            // TODO, special flen and decimal
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);
            let r = cast_time_as_string(&mut ctx, &extra, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_duration_as_string() {
        test_none_with_ctx_and_extra(cast_duration_as_string);

        let cs = vec![
            (
                Duration::parse("17:51:04.78".as_bytes(), 2).unwrap(),
                "17:51:04.78".to_string()
            ),
            (
                Duration::parse("-17:51:04.78".as_bytes(), 2).unwrap(),
                "-17:51:04.78".to_string()
            ),
            (
                Duration::parse("17:51:04.78".as_bytes(), 0).unwrap(),
                "17:51:04".to_string()
            ),
            (
                Duration::parse("-17:51:04.78".as_bytes(), 0).unwrap(),
                "-17:51:04".to_string()
            ),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type(false);
            let extra = make_extra(&rft, &ia);
            let r = cast_duration_as_string(&mut ctx, &extra, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_json_as_string() {
        test_none_with_ctx(cast_any_as_any::<Json, Bytes>);

        let cs: Vec<(Json, String)> = vec![
            // (origin, res)
            (
                Json::Object(BTreeMap::default()),
                "{}".to_string()
            ),
            (Json::Array(vec![]), "[]".to_string()),
            (Json::I64(10), "10".to_string()),
            (Json::I64(i64::MAX), i64::MAX.to_string()),
            (Json::I64(i64::MIN), i64::MIN.to_string()),
            (Json::U64(0), "0".to_string()),
            (Json::U64(u64::MAX), u64::MAX.to_string()),
            (
                Json::Double(i64::MIN as u64 as f64),
                (i64::MIN as u64 as f64).to_string()
            ),
            (
                Json::Double(i64::MAX as u64 as f64),
                (i64::MAX as u64 as f64).to_string()
            ),
            (
                Json::Double(i64::MIN as u64 as f64),
                (i64::MIN as u64 as f64).to_string(),
            ),
            (
                Json::Double(i64::MIN as f64),
                (i64::MIN as f64).to_string(),
            ),
            (Json::Double(10.5), "10.5".to_string()),
            (Json::Double(10.4), "10.4".to_string()),
            (Json::Double(-10.4), "-10.4".to_string()),
            (Json::Double(-10.5), "-10.5".to_string()),
            (
                Json::String(String::from("10.0")),
                "\"10.0\"".to_string()
            ),
            (Json::Boolean(true), "true".to_string()),
            (Json::Boolean(false), "false".to_string()),
            (Json::None, Json::None.to_string()),
        ];

        for (input, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Json, Bytes>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_int_as_decimal() {
        let cs = vec![
            (10, Decimal::from(10)),
            (10, Decimal::from(10u64)),
            (i64::MAX, Decimal::from(i64::MAX)),
            (i64::MIN, Decimal::from(i64::MIN)),
        ];

        let cs = vec![
            (10u64, Decimal::from(10u64)),
            (u64::MAX, Decimal::from(u64::MAX)),
            (0, Decimal::zero()),
        ];

        // TODO, signed int to unsigned decimal
    }

    #[test]
    fn test_real_as_decimal() {
        let cs = vec![
            (10.0, Decimal::from_f64(10.0)),
            (f64::MAX, Decimal::from_f64(f64::MAX)),
            (f64::MIN, Decimal::from_f64(f64::MIN)),
        ];

        // in_union
        let cs = vec![
            (-10.0, Decimal::zero()),
            (i64::MIN as f64, Decimal::zero()),
            (f64::MIN, Decimal::zero()),
        ];
    }

    #[test]
    fn test_string_as_decimal() {
        // in_union and result is unsigned
        let cs = vec![
            (
                "-1024",
                Decimal::zero(),
            ),
            (
                "1024",
                Decimal::from(1024),
            ),
        ];

        // TODO

        let cs = vec![
            (
                "18446744073709551615",
                Decimal::from(18446744073709551615u64)
            ),
            (
                "-18446744073709551616",
                Decimal::from_bytes("-18446744073709551616".as_bytes()).unwrap().unwrap()
            )
        ];

        // TODO

        // TODO, special flen and decimal
    }

    #[test]
    fn test_decimal_as_decimal() {
        // in_union and result is unsigned
        let cs = vec![
            // (origin, result)
            (Decimal::from_f64(-10f64).unwrap(), Decimal::zero()),
            (Decimal::from_f64(f64::MIN).unwrap(), Decimal::zero()),
            (Decimal::from_f64(10f64).unwrap(), Decimal::from_f64(10f64).unwrap()),
            (Decimal::from_f64(f64::MAX).unwrap(), Decimal::from_f64(f64::MAX).unwrap()),
        ];

        // TODO

        let cs = vec![
            (Decimal::from_f64(-10f64).unwrap(), Decimal::from_f64(-10f64).unwrap()),
            (Decimal::from_f64(f64::MIN).unwrap(), Decimal::from_f64(f64::MIN).unwrap()),
            (Decimal::from_f64(10f64).unwrap(), Decimal::from_f64(10f64).unwrap()),
            (Decimal::from_f64(f64::MAX).unwrap(), Decimal::from_f64(f64::MAX).unwrap()),
        ];

        // TODO

        // TODO special flen and decimal
    }

    #[test]
    fn test_time_as_decimal() {
        let cs = vec![
            ()
        ];
    }

    #[test]
    fn test_duration_as_decimal() {
        let cs = vec![
            (Duraition::from())
        ];
    }

    #[test]
    fn test_json_as_decimal() {
        let cs = vec![
            (Json::String(String("9999999999999999999")), Decimal::from_bytes("9999999999999999999".as_bytes())),
            (Json::String(String("-9999999999999999999")), Decimal::from_bytes("9999999999999999999".as_bytes())),
        ];
        // the case  below copy from test_json_as_real
        // no overflow
        let cs = vec![
            // (origin, res)
            (Json::Object(BTreeMap::default()), Decimal::zero()),
            (Json::Array(vec![]), Decimal::zero()),
            (Json::I64(10), 10f64),
            (Json::I64(i64::MAX), Decimal::from_f64(i64::MAX as f64)),
            (Json::I64(i64::MIN), Decimal::from(i64::MIN as f64)),
            (Json::U64(0), Decimal::zero()),
            (Json::U64(u64::MAX), Decimal::from_f64(u64::MAX as f64)),
            (Json::Double(f64::MAX), Decimal::from_f64(f64::MAX)),
            (Json::Double(f64::MIN), Decimal::from_f64(f64::MIN)),
            (Json::String(String::from("10.0")), Decimal::from_f64(10.0)),
            (Json::String(String::from("-10.0")), Decimal::from_f64(-10.0)),
            (Json::Boolean(true), Deciaml::from_f64(1f64)),
            (Json::Boolean(false), Decimal::zero()),
            (Json::None, Decimal::zero()),
        ];

        // TODO

        // truncated
        let cs = vec![
            (
                Json::String((0..500).map(|_| '9').collect::<String>()),
                Decimal::from_f64(f64::MAX),
            ),
            (
                Json::String(
                    (0..500).map(
                        |x| if x == 0 { '-' } else { '9' }
                    ).collect::<String>(),
                ),
                Decimal::from_f64(f64::MIN),
            ),
        ];
    }

    #[test]
    fn test_int_as_duration() {
        test_none_with_ctx_and_extra(cast_int_as_duration);

        let cs: Vec<(i64, isize, crate::codec::Result<Duration>, bool)> = vec![
            // (input, fsp, expect, overflow)
            (
                101010,
                0,
                Ok(Duration::parse("10:10:10".as_bytes(), 0).unwrap()),
                false,
            ),
            (
                101010,
                5,
                Ok(Duration::parse("10:10:10".as_bytes(), 5).unwrap()),
                false,
            ),
            (
                8385959,
                0,
                Ok(Duration::parse("838:59:59".as_bytes(), 0).unwrap()),
                false,
            ),
            (
                8385959,
                6,
                Ok(Duration::parse("838:59:59".as_bytes(), 6).unwrap()),
                false,
            ),
            (
                -101010,
                0,
                Ok(Duration::parse("-10:10:10".as_bytes(), 0).unwrap()),
                false,
            ),
            (
                -101010,
                5,
                Ok(Duration::parse("-10:10:10".as_bytes(), 5).unwrap()),
                false,
            ),
            (
                -8385959,
                0,
                Ok(Duration::parse("-838:59:59".as_bytes(), 0).unwrap()),
                false,
            ),
            (
                -8385959,
                6,
                Ok(Duration::parse("-838:59:59".as_bytes(), 6).unwrap()),
                false,
            ),
            // will overflow
            (
                8385960,
                0,
                Ok(Duration::parse("838:59:59".as_bytes(), 0).unwrap()),
                true,
            ),
            (
                8385960,
                1,
                Ok(Duration::parse("838:59:59".as_bytes(), 1).unwrap()),
                true,
            ),
            (
                8385960,
                5,
                Ok(Duration::parse("838:59:59".as_bytes(), 5).unwrap()),
                true,
            ),
            (
                8385960,
                6,
                Ok(Duration::parse("838:59:59".as_bytes(), 6).unwrap()),
                true,
            ),
            (
                -8385960,
                0,
                Ok(Duration::parse("-838:59:59".as_bytes(), 0).unwrap()),
                true,
            ),
            (
                -8385960,
                1,
                Ok(Duration::parse("-838:59:59".as_bytes(), 1).unwrap()),
                true,
            ),
            (
                -8385960,
                5,
                Ok(Duration::parse("-838:59:59".as_bytes(), 5).unwrap()),
                true,
            ),
            (
                -8385960,
                6,
                Ok(Duration::parse("-838:59:59".as_bytes(), 6).unwrap()),
                true,
            ),
            // will truncated
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8375960, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            // TODO, add test for num>=10000000000
            //  after Duration::from_f64 had impl logic for num>=10000000000
            // (10000000000, 0, Ok(Duration::parse("0:0:0".as_bytes(), 0).unwrap())),
            // (10000235959, 0, Ok(Duration::parse("23:59:59".as_bytes(), 0).unwrap())),
            // (10000000000, 0, Ok(Duration::parse("0:0:0".as_bytes(), 0).unwrap())),
        ];

        for (input, fsp, expect, overflow) in cs {
            let mut ctx = make_ctx(true, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_int_as_duration(&mut ctx, &extra, &Some(input));
            let expect_str = match expect.as_ref() {
                Ok(dur) => dur.to_string(),
                Err(e) => format!("{:?}", e),
            };
            let result_str = match r {
                Ok(Some(x)) => x.to_string(),
                _ => format!("{:?}", r),
            };
            let log = format!(
                "input: {}, fsp: {}, expect: {}, result: {:?}",
                input, fsp, expect_str, result_str
            );
            if overflow {
                assert!(r.is_ok(), "{}", log);
                assert!(r.unwrap().is_none(), "{}", log);
            } else {
                match expect {
                    Ok(expect) => check_result(&input, Some(&expect), &r),
                    Err(e) => assert_eq!(e.code(), WARN_DATA_TRUNCATED),
                }
            }
            check_overflow(&ctx, true);
        }
    }

    #[test]
    fn test_real_as_duration() {
        test_none_with_ctx_and_extra(cast_real_as_duration);

        // cast_real_as_duration call Duration::parse directly,
        // and Duration::parse is test in duration.rs.
        // Our test here is to make sure that the result is same as calling Duration::parse
        // no matter whether call_real_as_duration call Duration::parse directly.
        let cs: Vec<(f64, isize, Duration)> = vec![
            // (input, fsp, result)
            (
                101112.0,
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                101112.123456,
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                1112.0,
                0,
                Duration::parse("00:11:12".as_bytes(), 0).unwrap(),
            ),
            (12.0, 0, Duration::parse("00:00:12".as_bytes(), 0).unwrap()),
            (
                -0.123,
                3,
                Duration::parse("-00:00:00.123".as_bytes(), 3).unwrap(),
            ),
            (
                12345.0,
                0,
                Duration::parse("01:23:45".as_bytes(), 0).unwrap(),
            ),
            (
                -123.0,
                0,
                Duration::parse("-00:01:23".as_bytes(), 0).unwrap(),
            ),
            (
                -23.0,
                0,
                Duration::parse("-00:00:23".as_bytes(), 0).unwrap(),
            ),
        ];

        for (input, fsp, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_real_as_duration(&mut ctx, &extra, &Real::new(input).ok());
            check_result(&input, Some(&expect), &r);
        }
    }

    #[test]
    fn test_string_as_duration() {
        test_none_with_ctx_and_extra(cast_bytes_as_duration);

        let cs: Vec<(String, isize, Duration)> = vec![
            // (input, fsp, expect)
            (
                "10:11:12".to_string(),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "101112".to_string(),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "10:11".to_string(),
                0,
                Duration::parse("10:11:00".as_bytes(), 0).unwrap(),
            ),
            (
                "101112.123456".to_string(),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "1112".to_string(),
                0,
                Duration::parse("00:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "12".to_string(),
                0,
                Duration::parse("00:00:12".as_bytes(), 0).unwrap(),
            ),
            (
                "1 12".to_string(),
                0,
                Duration::parse("36:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "1 10:11:12".to_string(),
                0,
                Duration::parse("34:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "1 10:11:12.123456".to_string(),
                0,
                Duration::parse("34:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "1 10:11:12.123456".to_string(),
                4,
                Duration::parse("34:11:12.1235".as_bytes(), 4).unwrap(),
            ),
            (
                "1 10:11:12.12".to_string(),
                4,
                Duration::parse("34:11:12.1200".as_bytes(), 6).unwrap(),
            ),
            (
                "1 10:11:12.1234565".to_string(),
                6,
                Duration::parse("34:11:12.123457".as_bytes(), 6).unwrap(),
            ),
            (
                "1 10:11:12.9999995".to_string(),
                6,
                Duration::parse("34:11:13.000000".as_bytes(), 6).unwrap(),
            ),
            (
                "10:11:12.123456".to_string(),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                "1 10:11".to_string(),
                0,
                Duration::parse("34:11:00".as_bytes(), 0).unwrap(),
            ),
            (
                "1 10".to_string(),
                0,
                Duration::parse("34:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "24 10".to_string(),
                0,
                Duration::parse("586:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "-24 10".to_string(),
                0,
                Duration::parse("-586:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "0 10".to_string(),
                0,
                Duration::parse("10:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "-10:10:10".to_string(),
                0,
                Duration::parse("-10:10:10".as_bytes(), 0).unwrap(),
            ),
            (
                "-838:59:59".to_string(),
                0,
                Duration::parse("-838:59:59".as_bytes(), 0).unwrap(),
            ),
            (
                "838:59:59".to_string(),
                0,
                Duration::parse("838:59:59".as_bytes(), 0).unwrap(),
            ),
            (
                "54:59:59".to_string(),
                0,
                Duration::parse("54:59:59".as_bytes(), 0).unwrap(),
            ),
            (
                "00:00:00.1".to_string(),
                0,
                Duration::parse("00:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "00:00:00.1".to_string(),
                1,
                Duration::parse("00:00:00.1".as_bytes(), 1).unwrap(),
            ),
            (
                "00:00:00.777777".to_string(),
                2,
                Duration::parse("00:00:00.78".as_bytes(), 2).unwrap(),
            ),
            (
                "00:00:00.777777".to_string(),
                6,
                Duration::parse("00:00:00.777777".as_bytes(), 6).unwrap(),
            ),
            (
                "00:00:00.001".to_string(),
                3,
                Duration::parse("00:00:00.001".as_bytes(), 3).unwrap(),
            ),
            (
                "- 1 ".to_string(),
                0,
                Duration::parse("-00:00:01".as_bytes(), 0).unwrap(),
            ),
            (
                "1:2:3".to_string(),
                0,
                Duration::parse("01:02:03".as_bytes(), 0).unwrap(),
            ),
            (
                "1 1:2:3".to_string(),
                0,
                Duration::parse("25:02:03".as_bytes(), 0).unwrap(),
            ),
            (
                "-1 1:2:3.123".to_string(),
                3,
                Duration::parse("-25:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                "-.123".to_string(),
                3,
                Duration::parse("-00:00:00.123".as_bytes(), 0).unwrap(),
            ),
            (
                "12345".to_string(),
                0,
                Duration::parse("01:23:45".as_bytes(), 0).unwrap(),
            ),
            (
                "-123".to_string(),
                0,
                Duration::parse("-00:01:23".as_bytes(), 0).unwrap(),
            ),
            (
                "-23".to_string(),
                0,
                Duration::parse("-00:00:23".as_bytes(), 0).unwrap(),
            ),
            (
                "- 1 1".to_string(),
                0,
                Duration::parse("-25:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                "-1 1".to_string(),
                0,
                Duration::parse("-25:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                " - 1:2:3 .123 ".to_string(),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                " - 1 :2 :3 .123 ".to_string(),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                " - 1 : 2 :3 .123 ".to_string(),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                " - 1 : 2 :  3 .123 ".to_string(),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                " - 1 .123 ".to_string(),
                3,
                Duration::parse("-00:00:01.123".as_bytes(), 3).unwrap(),
            ),
            (
                "".to_string(),
                0,
                Duration::parse("00:00:00".as_bytes(), 0).unwrap(),
            ),
        ];

        for (input, fsp, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(true);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_bytes_as_duration(&mut ctx, &extra, &Some(input.clone().into_bytes()));
            check_result(&input, Some(&expect), &r);
        }

        let cs: Vec<(String, isize, crate::codec::Error)> = vec![
            // (input, fps, error)
            (
                "1 10:11:12.123456".to_string(),
                7,
                Error::InvalidDataType(String::new()),
            ),
            ("".to_string(), 7, Error::InvalidDataType(String::new())),
            (
                "18446744073709551615:59:59".to_string(),
                0,
                Error::overflow("", ""),
            ),
            (
                "1::2:3".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "1.23 3".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "-".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "23:60:59".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "2011-11-11 00:00:01".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "2011-11-11".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "--23".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "232 10".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                "-232 10".to_string(),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
        ];
        for (input, fsp, err) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(true);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_bytes_as_duration(&mut ctx, &extra, &Some(input.clone().into_bytes()));
            let log = format!(
                "input: {}, fsp: {}, expect_err: {:?}, got: {:?}",
                input, fsp, err, r
            );
            match err {
                Error::InvalidDataType(_) => assert!(r.is_err(), "{}", log),
                _ => {
                    assert!(r.is_ok(), "{}", log);
                    assert!(r.unwrap().unwrap().is_zero(), "{}", log);
                    check_overflow(&ctx, err.is_overflow());
                    check_truncated(&ctx, err.code() == ERR_TRUNCATE_WRONG_VALUE);
                }
            }
        }
    }

    #[test]
    fn test_decimal_as_duration() {
        test_none_with_ctx_and_extra(cast_decimal_as_duration);

        let cs: Vec<(Decimal, isize, Duration)> = vec![
            // (input, fsp, result)
            (
                Decimal::from_f64(101112.0).unwrap(),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Decimal::from_f64(101112.123456).unwrap(),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Decimal::from_f64(1112.0).unwrap(),
                0,
                Duration::parse("00:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Decimal::from_f64(12.0).unwrap(),
                0,
                Duration::parse("00:00:12".as_bytes(), 0).unwrap(),
            ),
            (
                Decimal::from_f64(-0.123).unwrap(),
                3,
                Duration::parse("-00:00:00.123".as_bytes(), 3).unwrap(),
            ),
            (
                Decimal::from_f64(12345.0).unwrap(),
                0,
                Duration::parse("01:23:45".as_bytes(), 0).unwrap(),
            ),
            (
                Decimal::from_f64(-123.0).unwrap(),
                0,
                Duration::parse("-00:01:23".as_bytes(), 0).unwrap(),
            ),
            (
                Decimal::from_f64(-23.0).unwrap(),
                0,
                Duration::parse("-00:00:23".as_bytes(), 0).unwrap(),
            ),
        ];
        // TODO, add test case for truncated case
        for (input, fsp, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_decimal_as_duration(&mut ctx, &extra, &Some(input.clone()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_time_as_duration() {
        test_none_with_ctx_and_extra(cast_time_as_duration);

        // TODO, add more test case
        let cs: Vec<(Time, isize, Duration)> = vec![
            // (input, fsp, result)
            (
                Time::parse_utc_datetime("11:11:11", 0).unwrap(),
                0,
                Duration::parse("11:11:11".as_bytes(), 0).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.1111", 4).unwrap(),
                4,
                Duration::parse("11:11:11.1111".as_bytes(), 4).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.2222", 4).unwrap(),
                4,
                Duration::parse("11:11:11.2222".as_bytes(), 4).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.3333", 4).unwrap(),
                4,
                Duration::parse("11:11:11.3333".as_bytes(), 4).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.1111", 4).unwrap(),
                3,
                Duration::parse("11:11:11.1111".as_bytes(), 3).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.2222", 4).unwrap(),
                3,
                Duration::parse("11:11:11.2222".as_bytes(), 3).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.3333", 4).unwrap(),
                3,
                Duration::parse("11:11:11.3333".as_bytes(), 3).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.1111", 2).unwrap(),
                2,
                Duration::parse("11:11:11.1111".as_bytes(), 2).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.2222", 2).unwrap(),
                2,
                Duration::parse("11:11:11.2222".as_bytes(), 2).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.3333", 2).unwrap(),
                2,
                Duration::parse("11:11:11.3333".as_bytes(), 2).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.1111", 2).unwrap(),
                3,
                Duration::parse("11:11:11.11".as_bytes(), 3).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.2222", 2).unwrap(),
                3,
                Duration::parse("11:11:11.22".as_bytes(), 3).unwrap(),
            ),
            (
                Time::parse_utc_datetime("11:11:11.3333", 2).unwrap(),
                3,
                Duration::parse("11:11:11.33".as_bytes(), 3).unwrap(),
            ),
        ];
        // TODO, add test case for truncated case

        for (input, fsp, result) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_time_as_duration(&mut ctx, &extra, &Some(input.clone()));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_duration_as_duration() {
        test_none_with_extra(cast_duration_as_duration);

        let cs = vec![
            // (input, fsp, result)
            (
                Duration::parse("12:00:23.77777".as_bytes(), 6).unwrap(),
                5,
                Duration::parse("12:00:23.77777".as_bytes(), 5)
                    .unwrap()
                    .round_frac(5)
                    .unwrap(),
            ),
            (
                Duration::parse("12:00:23.77777".as_bytes(), 6).unwrap(),
                5,
                Duration::parse("12:00:23.77777".as_bytes(), 5).unwrap(),
            ),
            (
                Duration::parse("12:00:23.11111".as_bytes(), 6).unwrap(),
                0,
                Duration::parse("12:00:23".as_bytes(), 0).unwrap(),
            ),
            (
                Duration::parse("12:00:23.77777".as_bytes(), 5).unwrap(),
                6,
                Duration::parse("12:00:23.77777".as_bytes(), 5)
                    .unwrap()
                    .round_frac(6)
                    .unwrap(),
            ),
            (
                Duration::parse("12:00:23.77777".as_bytes(), 4).unwrap(),
                5,
                Duration::parse("12:00:23.77777".as_bytes(), 4)
                    .unwrap()
                    .round_frac(5)
                    .unwrap(),
            ),
            (
                Duration::parse("12:00:23.11111".as_bytes(), 6).unwrap(),
                0,
                Duration::parse("12:00:23.11111".as_bytes(), 6)
                    .unwrap()
                    .round_frac(0)
                    .unwrap(),
            ),
        ];
        // TODO, add test case for truncated case

        for (input, fsp, result) in cs {
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_duration_as_duration(&extra, &Some(input));
            check_result(&input, Some(&result), &r);
        }
    }

    #[test]
    fn test_json_as_duration() {
        test_none_with_ctx_and_extra(cast_json_as_duration);

        let cs: Vec<(Json, isize, Duration)> = vec![
            // (input, fsp, expect)
            (
                Json::String("10:11:12".to_string()),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("101112".to_string()),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("10:11".to_string()),
                0,
                Duration::parse("10:11:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("101112.123456".to_string()),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1112".to_string()),
                0,
                Duration::parse("00:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("12".to_string()),
                0,
                Duration::parse("00:00:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 12".to_string()),
                0,
                Duration::parse("36:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 10:11:12".to_string()),
                0,
                Duration::parse("34:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 10:11:12.123456".to_string()),
                0,
                Duration::parse("34:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 10:11:12.123456".to_string()),
                4,
                Duration::parse("34:11:12.1235".as_bytes(), 4).unwrap(),
            ),
            (
                Json::String("1 10:11:12.12".to_string()),
                4,
                Duration::parse("34:11:12.1200".as_bytes(), 6).unwrap(),
            ),
            (
                Json::String("1 10:11:12.1234565".to_string()),
                6,
                Duration::parse("34:11:12.123457".as_bytes(), 6).unwrap(),
            ),
            (
                Json::String("1 10:11:12.9999995".to_string()),
                6,
                Duration::parse("34:11:13.000000".as_bytes(), 6).unwrap(),
            ),
            (
                Json::String("10:11:12.123456".to_string()),
                0,
                Duration::parse("10:11:12".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 10:11".to_string()),
                0,
                Duration::parse("34:11:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 10".to_string()),
                0,
                Duration::parse("34:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("24 10".to_string()),
                0,
                Duration::parse("586:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-24 10".to_string()),
                0,
                Duration::parse("-586:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("0 10".to_string()),
                0,
                Duration::parse("10:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-10:10:10".to_string()),
                0,
                Duration::parse("-10:10:10".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-838:59:59".to_string()),
                0,
                Duration::parse("-838:59:59".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("838:59:59".to_string()),
                0,
                Duration::parse("838:59:59".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("54:59:59".to_string()),
                0,
                Duration::parse("54:59:59".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("00:00:00.1".to_string()),
                0,
                Duration::parse("00:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("00:00:00.1".to_string()),
                1,
                Duration::parse("00:00:00.1".as_bytes(), 1).unwrap(),
            ),
            (
                Json::String("00:00:00.777777".to_string()),
                2,
                Duration::parse("00:00:00.78".as_bytes(), 2).unwrap(),
            ),
            (
                Json::String("00:00:00.777777".to_string()),
                6,
                Duration::parse("00:00:00.777777".as_bytes(), 6).unwrap(),
            ),
            (
                Json::String("00:00:00.001".to_string()),
                3,
                Duration::parse("00:00:00.001".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String("- 1 ".to_string()),
                0,
                Duration::parse("-00:00:01".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1:2:3".to_string()),
                0,
                Duration::parse("01:02:03".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("1 1:2:3".to_string()),
                0,
                Duration::parse("25:02:03".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-1 1:2:3.123".to_string()),
                3,
                Duration::parse("-25:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String("-.123".to_string()),
                3,
                Duration::parse("-00:00:00.123".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("12345".to_string()),
                0,
                Duration::parse("01:23:45".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-123".to_string()),
                0,
                Duration::parse("-00:01:23".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-23".to_string()),
                0,
                Duration::parse("-00:00:23".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("- 1 1".to_string()),
                0,
                Duration::parse("-25:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String("-1 1".to_string()),
                0,
                Duration::parse("-25:00:00".as_bytes(), 0).unwrap(),
            ),
            (
                Json::String(" - 1:2:3 .123 ".to_string()),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String(" - 1 :2 :3 .123 ".to_string()),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String(" - 1 : 2 :3 .123 ".to_string()),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String(" - 1 : 2 :  3 .123 ".to_string()),
                3,
                Duration::parse("-01:02:03.123".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String(" - 1 .123 ".to_string()),
                3,
                Duration::parse("-00:00:01.123".as_bytes(), 3).unwrap(),
            ),
            (
                Json::String("".to_string()),
                0,
                Duration::parse("00:00:00".as_bytes(), 0).unwrap(),
            ),
        ];

        for (input, fsp, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let ia = make_implicit_args(true);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_json_as_duration(&mut ctx, &extra, &Some(input.clone()));
            check_result(&input, Some(&expect), &r);
        }

        let cs: Vec<(Json, isize, crate::codec::Error)> = vec![
            // (input, fps, err)
            (
                Json::String("1 10:11:12.123456".to_string()),
                7,
                Error::InvalidDataType(String::new()),
            ),
            (
                Json::String("".to_string()),
                7,
                Error::InvalidDataType(String::new()),
            ),
            (
                Json::String("18446744073709551615:59:59".to_string()),
                0,
                Error::overflow("", ""),
            ),
            (
                Json::String("1::2:3".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("1.23 3".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("-".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("23:60:59".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("2011-11-11 00:00:01".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("2011-11-11".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("--23".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("232 10".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
            (
                Json::String("-232 10".to_string()),
                0,
                Error::truncated_wrong_val("time", String::new()),
            ),
        ];
        for (input, fsp, err) in cs {
            let mut ctx = make_ctx(true, true, false);
            let ia = make_implicit_args(true);
            let rft = make_ret_field_type_2(false, UNSPECIFIED_LENGTH, fsp);
            let extra = make_extra(&rft, &ia);
            let r = cast_json_as_duration(&mut ctx, &extra, &Some(input.clone()));
            let log = format!(
                "input: {}, fsp: {}, expect_err: {:?}, got: {:?}",
                input, fsp, err, r
            );
            match err {
                Error::InvalidDataType(_) => assert!(r.is_err(), "{}", log),
                _ => {
                    assert!(r.is_ok(), "{}", log);
                    assert!(r.unwrap().unwrap().is_zero(), "{}", log);
                    check_overflow(&ctx, err.is_overflow());
                    check_truncated(&ctx, err.code() == ERR_TRUNCATE_WRONG_VALUE);
                }
            }
        }
    }

    fn test_bool_as_json() {
        test_none_with_nothing(cast_bool_as_json);

        let cs: Vec<(i64, Json)> = vec![
            // (input, result)
            (0, Json::Boolean(false)),
            (1, Json::Boolean(true)),
            (-1, Json::Boolean(true)),
            (i64::MIN, Json::Boolean(true)),
            (i64::MAX, Json::Boolean(true)),
        ];
        for (input, result) in cs {
            let r = cast_bool_as_json(&Some(input.clone()));
            check_result(&input, Some(&result), &r);
        }
    }

    fn test_int_as_json() {
        test_none_with_ctx(cast_any_as_any::<Int, Json>);

    }

    fn test_uint_as_json() {
        test_none_with_nothing(cast_uint_as_json);
        let cs: Vec<(u64, Json)> = vec![
            // (input, result)
            (0, Json::U64(0)),
            (u64::MAX, Json::U64(u64::MAX)),
            (i64::MAX as u64, Json::U64(i64::MAX as u64))
        ];

        for (input, expect) in cs {
            let r = cast_uint_as_json(&Some(input));
            check_result(&input, Some(&expect), &r);
        }
    }

    #[test]
    fn test_real_as_json() {
        let cs = (None, Ok(None));

        let cs = vec![
            (f64::MAX, Json::Double(f64::MAX)),
            (f64::MIN, Json::Double(f64::MIN)),
        ];
        // TODO
    }

    #[test]
    fn test_string_as_json() {
        let mut jo1: BTreeMap<String, Json> = BTreeMap::new();
        jo1.insert(String::from("a"), Json::String(String::from("b")));
        // TODO, check this case
        // HasParseToJSONFlag
        let cs = vec![
            ("{\"a\": \"b\"}", Json::Object(jo1)),
            ("[1, 2, 3]", Json::Array(vec![Json::I64(1), Json::I64(2), Json::I64(3)])),
            ("10", Json::I64(10)),
            ("9223372036854775807", Json::I64(9223372036854775807)),
            ("-9223372036854775807", Json::I64(-9223372036854775808)),
            ("-9223372036854775807", Json::I64(-9223372036854775808)),
            ("18446744073709551615", Json::U64(18446744073709551615)),
            (f64::MAX.to_string().as_str(), Json::Double(f64::MAX)),
            (f64::MIN.to_string().as_str(), Json::Double(f64::MIN)),
            ("0.0", Json::Double(0.0)),
            ("abcde", Json::String(String::from("abcde"))),
            ("true", Json::Boolean(true)),
            ("", Json::None),
        ];
    }

    #[test]
    fn test_decimal_as_json() {
        let cs = vec![
            (Decimal::from_f64(999999999999.99999999999999), Json::F64(999999999999.99999999999999)),
            (Decimal::from(10), Json::F64(10.0)),
            (Decimal::from(-10), Json::F64(-10.0)),
            (Decimal::from(10u64), Json::F64(10.0)),
        ];
    }

    #[test]
    fn test_time_as_json() {
        let cs = vec![
            (Time::parse_utc_datetime("2019-09-01", 0), Json::String(String::from("2019-09-01"))),
            (Time::parse_utc_datetime("2019-09-01", 0), Json::String(String::from("2019-09-01"))),
            (Time::parse_utc_datetime("2019-09-01", 0), Json::String(String::from("2019-09-01"))),
            (Time::parse_utc_datetime("2019-09-01", 0), Json::String(String::from("2019-09-01"))),
            (Time::parse_utc_datetime("2019-09-01", 0), Json::String(String::from("2019-09-01"))),
            (Time::parse_utc_datetime("2019-09-01", 0), Json::String(String::from("2019-09-01"))),
        ];
    }

    #[test]
    fn test_duration_as_json() {
        // TODO, add more case
        let cs = vec![
            (Duration::zero(), Json::String(Duration::zero().to_string())),
            (Duration::parse("10:10:10".as_bytes(), 0).unwrap(), Json::String("10:10:10".to_string())),
        ];
    }

    #[test]
    fn test_json_as_json() {
        let mut jo1: BTreeMap<String, Json> = BTreeMap::new();
        jo1.insert(String::from("a"), Json::String(String::from("b")));
        let cs = vec![
            Json::Object(jo1),
            Json::Array(vec![Json::I64(1), Json::I64(3), Json::I64(4)]),
            Json::I64(i64::MIN),
            Json::I64(i64::MAX),
            Json::U64(0u64),
            Json::U64(u64::MAX),
            Json::Double(f64::MIN),
            Json::Double(f64::MAX),
            Json::String(String::from("abcde")),
            Json::Boolean(true),
            Json::Boolean(false),
            Json::None,
        ];
    }
}
