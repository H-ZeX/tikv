// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryFrom;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
use crate::codec::mysql::Time;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use crate::Result;

fn get_cast_fn_rpn_meta(
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<RpnFnMeta> {
    let from = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
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

        (EvalType::Int, EvalType::Decimal) => {
            if !from_field_type.is_unsigned() && !to_field_type.is_unsigned() {
                cast_any_as_decimal_fn_meta::<Int>()
            } else {
                cast_uint_as_decimal_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Bytes>(),
        (EvalType::Real, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Real>(),
        (EvalType::DateTime, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<DateTime>(),
        (EvalType::Duration, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Duration>(),
        (EvalType::Json, EvalType::Decimal) => cast_any_as_decimal_fn_meta::<Json>(),
        (EvalType::Int, EvalType::Int) => {
            match (from_field_type.is_unsigned(), to_field_type.is_unsigned()) {
                (false, false) => cast_any_as_any_fn_meta::<Int, Int>(),
                (false, true) => cast_int_as_uint_fn_meta(),
                (true, false) => cast_uint_as_int_fn_meta(),
                (true, true) => cast_uint_as_uint_fn_meta(),
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Real, Int>()
            } else {
                cast_float_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Bytes, Int>()
            } else {
                cast_bytes_as_uint_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Decimal, Int>()
            } else {
                cast_decimal_as_uint_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<DateTime, Int>()
            } else {
                cast_datetime_as_uint_fn_meta()
            }
        }
        (EvalType::Duration, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Duration, Int>()
            } else {
                cast_duration_as_uint_fn_meta()
            }
        }
        (EvalType::Json, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Json, Int>()
            } else {
                cast_json_as_uint_fn_meta()
            }
        }
        (EvalType::Int, EvalType::Json) => {
            if from_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::IS_BOOLEAN)
            {
                cast_int_as_json_boolean_fn_meta()
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
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_bytes_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::DateTime, EvalType::Duration) => cast_any_as_any_fn_meta::<DateTime, Duration>(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),
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

/// The unsigned int implementation for push down signature `CastIntAsDecimal`.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_uint_as_decimal(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<i64>,
) -> Result<Option<Decimal>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // TODO, TiDB's uint to decimal seems has bug, fix this after fix TiDB's
            let dec = if in_union(extra.implicit_args) && *val < 0 {
                Decimal::zero()
            } else {
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

/// The signed int implementation for push down signature `CastIntAsDecimal`.
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_any_as_decimal<From: Evaluable + ConvertTo<Decimal>>(
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

#[rpn_fn]
#[inline]
fn cast_uint_as_int(val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // the val is uint, so it will never < 0,
            // then we needn't to check whether in_union.
            //
            // needn't to call convert_uint_as_int
            Ok(Some(*val as i64))
        }
    }
}

macro_rules! cast_as_unsigned_integer {
    ($ty:ty, $as_uint_fn:ident) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, val,);
    };
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $extra,);
    };
    ($ty:ty, $as_uint_fn:ident, $extra:expr, $($hook:tt)*) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $extra, $($hook)*);
    };
    (_inner, $ty:ty, $as_uint_fn:ident, $extra:expr, $($hook:tt)*) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        #[allow(unused)]
        pub fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<i64>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    $($hook)*;
                    let val = ($extra).to_uint(ctx, FieldTypeTp::LongLong)?;
                    Ok(Some(val as i64))
                }
            }
        }
    };
}

cast_as_unsigned_integer!(
    Int,
    cast_int_as_uint,
    *val,
    if *val < 0 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Int, cast_uint_as_uint, *val as u64);
cast_as_unsigned_integer!(
    Real,
    cast_float_as_uint,
    val.into_inner(),
    if val.into_inner() < 0f64 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Bytes, cast_bytes_as_uint);
cast_as_unsigned_integer!(
    Decimal,
    cast_decimal_as_uint,
    val,
    if val.is_negative() && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(DateTime, cast_datetime_as_uint);
cast_as_unsigned_integer!(Duration, cast_duration_as_uint);
cast_as_unsigned_integer!(Json, cast_json_as_uint);

/// The implementation for push down signature `CastIntAsJson` from unsigned integer.
#[rpn_fn]
#[inline]
pub fn cast_uint_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::U64(*val as u64))),
    }
}

#[rpn_fn]
#[inline]
pub fn cast_int_as_json_boolean(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::Boolean(*val != 0))),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
pub fn cast_string_as_json(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<Json>> {
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

/// The implementation for push down signature `CastIntAsDuration`
#[rpn_fn(capture = [ctx, extra])]
#[inline]
pub fn cast_int_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dur = Duration::from_i64(ctx, *val, extra.ret_field_type.get_decimal() as u8)?;
            Ok(Some(dur))
        }
    }
}

macro_rules! cast_as_duration {
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        pub fn $as_uint_fn(
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
cast_as_duration!(Json, cast_json_as_duration, val.unquote()?.as_bytes());
cast_as_duration!(
    Decimal,
    cast_decimal_as_duration,
    val.to_string().as_bytes()
);

#[cfg(test)]
mod tests {
    use super::Result;
    use crate::codec::data_type::{Bytes, Decimal, Real, ScalarValue};
    use crate::codec::error::ERR_DATA_TOO_LONG;
    use crate::codec::mysql::charset::*;
    use crate::codec::mysql::{Duration, Json, Time};
    use crate::expr::Flag;
    use crate::expr::{EvalConfig, EvalContext};
    use crate::rpn_expr::impl_cast::*;
    use crate::rpn_expr::RpnFnCallExtra;
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Formatter, Display};
    use std::sync::Arc;
    use std::{f32, f64, i64, u64};
    use tidb_query_datatype::{
        Collation, FieldTypeAccessor, FieldTypeTp, UNSPECIFIED_LENGTH,
    };

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

    fn make_ret_field_type_3(
        flen: isize,
        charset: &str,
        tp: FieldTypeTp,
        collation: Collation,
    ) -> FieldType {
        let mut ft = FieldType::default();
        let fta = ft.as_mut_accessor();
        fta.set_flen(flen);
        fta.set_tp(tp);
        fta.set_collation(collation);
        ft.set_charset(String::from(charset));
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

    fn make_log<P: Display, R: Debug>(input: &P, expect: &R, result: &Result<Option<R>>) -> String {
        format!(
            "input: {}, expect: {:?}, output: {:?}",
            input, expect, result
        )
    }

    fn check_warning(ctx: &EvalContext, err_code: Option<i32>, log: &str) {
        if let Some(x) = err_code {
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "{}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
            assert_eq!(ctx.warnings.warnings[0].get_code(), x, "{}", log);
        } else {
            assert_eq!(
                ctx.warnings.warning_cnt, 0,
                "{}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
        }
    }

    fn check_result<R: Debug + PartialEq>(expect: Option<&R>, res: &Result<Option<R>>, log: &str) {
        assert!(res.is_ok(), "{}", log);
        let res = res.as_ref().unwrap();
        if res.is_none() {
            assert!(expect.is_none(), "{}", log);
        } else {
            let res = res.as_ref().unwrap();
            assert_eq!(res, expect.unwrap(), "{}", log);
        }
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

    /// base_cs:
    /// vector of (T, T to bytes(without any other handle do by cast_as_string_helper),
    /// T to string for debug output),
    /// the object should not be zero len.
    fn test_as_string_helper<T: Clone, FnCast>(
        base_cs: Vec<(T, Vec<u8>, String)>,
        cast_func: FnCast,
        func_name: &str,
    ) where
        FnCast: Fn(&mut EvalContext, &RpnFnCallExtra, &Option<T>) -> Result<Option<Bytes>>,
    {
        #[derive(Clone, Copy)]
        enum FlenType {
            Eq,
            LessOne,
            ExtraOne,
            Unspecified,
        }
        let cs: Vec<(FlenType, bool, &str, FieldTypeTp, Collation, Option<i32>)> = vec![
            // (flen_type, pad_zero, charset, tp, collation, err_code)

            // normal, flen==str.len
            (
                FlenType::Eq,
                false,
                CHARSET_BIN,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_ASCII,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_LATIN1,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            // normal, flen==UNSPECIFIED_LENGTH
            (
                FlenType::Unspecified,
                false,
                CHARSET_BIN,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_ASCII,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::Unspecified,
                false,
                CHARSET_LATIN1,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            // branch 1 of ProduceStrWithSpecifiedTp
            // not bin_str, so no pad_zero
            (
                FlenType::LessOne,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::LessOne,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                None,
            ),
            (
                FlenType::Eq,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                None,
            ),
            (
                FlenType::ExtraOne,
                false,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                None,
            ),
            // bin_str, so need pad_zero
            (
                FlenType::ExtraOne,
                true,
                CHARSET_UTF8,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            (
                FlenType::ExtraOne,
                true,
                CHARSET_UTF8MB4,
                FieldTypeTp::String,
                Collation::Binary,
                None,
            ),
            // branch 2 of ProduceStrWithSpecifiedTp
            // branch 2 need s.len>flen, so never need pad_zero
            (
                FlenType::LessOne,
                false,
                CHARSET_ASCII,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::LessOne,
                false,
                CHARSET_LATIN1,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                Some(ERR_DATA_TOO_LONG),
            ),
            (
                FlenType::LessOne,
                false,
                CHARSET_BIN,
                FieldTypeTp::String,
                Collation::UTF8Bin,
                Some(ERR_DATA_TOO_LONG),
            ),
            // branch 3 of ProduceStrWithSpecifiedTp ,
            // will never be reached,
            // because padZero param is always false
        ];
        for (input, bytes, debug_str) in base_cs {
            for (flen_type, pad_zero, charset, tp, collation, err_code) in cs.iter() {
                let mut ctx = make_ctx(false, true, false);
                let ia = make_implicit_args(false);
                let res_len = bytes.len();
                let flen = match flen_type {
                    FlenType::Eq => res_len as isize,
                    FlenType::LessOne => {
                        if res_len == 0 {
                            continue;
                        } else {
                            (res_len - 1) as isize
                        }
                    }
                    FlenType::ExtraOne => (res_len + 1) as isize,
                    FlenType::Unspecified => UNSPECIFIED_LENGTH,
                };
                let rft = make_ret_field_type_3(flen, charset, *tp, *collation);
                let extra = make_extra(&rft, &ia);

                let r = cast_func(&mut ctx, &extra, &Some(input.clone()));

                let mut expect = bytes.clone();
                if *pad_zero && flen > expect.len() as isize {
                    expect.extend((expect.len()..flen as usize).map(|_| 0u8));
                } else if flen != UNSPECIFIED_LENGTH {
                    expect.truncate(flen as usize);
                }

                let log = format!(
                    "func: {:?}, input: {}, expect: {:?}, flen: {}, \
                    charset: {}, field_type: {}, collation: {}, output: {:?}",
                    func_name, debug_str, &expect, flen, charset, tp, collation, &r
                );
                check_result(Some(&expect), &r, log.as_str());
                check_warning(&ctx, *err_code, log.as_str());
            }
        }
    }

    #[test]
    fn test_int_as_string() {
        test_none_with_ctx_and_extra(cast_int_as_string);

        let cs: Vec<(i64, Vec<u8>, String)> = vec![
            (i64::MAX, i64::MAX.to_string().into_bytes(), i64::MAX.to_string()),
            (i64::MIN, i64::MIN.to_string().into_bytes(), i64::MIN.to_string())
        ];
        test_as_string_helper(
            cs,
            cast_int_as_string,
            "cast_int_as_string",
        );
    }

    #[test]
    fn test_uint_as_string() {
        test_none_with_ctx_and_extra(cast_int_as_string);

        let cs: Vec<(u64, Vec<u8>, String)> = vec![
            (i64::MAX as u64, (i64::MAX as u64).to_string().into_bytes(), (i64::MAX as u64).to_string()),
            (i64::MIN as u64, (i64::MIN as u64).to_string().into_bytes(), (i64::MIN as u64).to_string()),
            (u64::MAX, u64::MAX.to_string().into_bytes(), u64::MAX.to_string()),
            (0u64, 0u64.to_string().into_bytes(), 0u64.to_string())
        ];
        test_as_string_helper(
            cs,
            |ctx, extra, val| {
                let val = val.map(|x| x as i64);
                cast_uint_as_string(ctx, extra, &val)
            },
            "cast_uint_as_string",
        );
    }

    #[test]
    fn test_float_real_as_string() {
        test_none_with_ctx_and_extra(cast_float_real_as_string);

        let cs: Vec<(f32, Vec<u8>, String)> = vec![
            (f32::MAX, f32::MAX.to_string().into_bytes(), f32::MAX.to_string()),
            (1.0f32, 1.0f32.to_string().into_bytes(), 1.0f32.to_string()),
            (1.1113f32, 1.1113f32.to_string().into_bytes(), 1.1113f32.to_string()),
            (0.1f32, 0.1f32.to_string().into_bytes(), 0.1f32.to_string())
        ];

        test_as_string_helper(
            cs,
            |ctx, extra, val| {
                cast_float_real_as_string(ctx, extra, &val.map(|x| Real::new(f64::from(x)).unwrap()))
            },
            "cast_float_real_as_string",
        );
    }

    #[test]
    fn test_double_real_as_string() {
        test_none_with_ctx_and_extra(cast_double_real_as_string);

        let cs: Vec<(f64, Vec<u8>, String)> = vec![
            (f64::from(f32::MAX), (f64::from(f32::MAX)).to_string().into_bytes(), f64::from(f32::MAX).to_string()),
            (f64::from(f32::MIN), (f64::from(f32::MIN)).to_string().into_bytes(), f64::from(f32::MIN).to_string()),
            (f64::MIN, f64::MIN.to_string().into_bytes(), f64::MIN.to_string()),
            (f64::MAX, f64::MAX.to_string().into_bytes(), f64::MAX.to_string()),
            (1.0f64, 1.0f64.to_string().into_bytes(), 1.0f64.to_string()),
            (1.1113f64, 1.1113f64.to_string().into_bytes(), 1.1113f64.to_string()),
            (0.1f64, 0.1f64.to_string().into_bytes(), 0.1f64.to_string())
        ];

        test_as_string_helper(
            cs,
            |ctx, extra, val| {
                cast_double_real_as_string(ctx, extra, &val.map(|x| Real::new(x).unwrap()))
            },
            "cast_double_real_as_string",
        );
    }

    #[test]
    fn test_string_as_string() {
        test_none_with_ctx_and_extra(cast_string_as_string);

        let cs: Vec<(Vec<u8>, Vec<u8>, String)> = vec![
            (Vec::from(b"".as_ref()), Vec::from(b"".as_ref()), String::from("<empty-str>")),
            ((0..1024).map(|_| b'0').collect::<Vec<u8>>(),
             (0..1024).map(|_| b'0').collect::<Vec<u8>>(),
             String::from("1024 zeros('0')"))
        ];

        test_as_string_helper(
            cs,
            cast_string_as_string,
            "cast_string_as_string",
        );
    }

    #[test]
    fn test_decimal_as_string() {
        test_none_with_ctx_and_extra(cast_decimal_as_string);

        let cs: Vec<(Decimal, Vec<u8>, String)> = vec![
            (Decimal::from(i64::MAX), i64::MAX.to_string().into_bytes(), i64::MAX.to_string()),
            (Decimal::from(i64::MIN), i64::MIN.to_string().into_bytes(), i64::MIN.to_string()),
            (Decimal::from(u64::MAX), u64::MAX.to_string().into_bytes(), u64::MAX.to_string()),
            (Decimal::from_f64(0.0).unwrap(), 0.0.to_string().into_bytes(), 0.0.to_string()),
            (Decimal::from_f64(i64::MAX as f64).unwrap(), (i64::MAX as f64).to_string().into_bytes(), (i64::MAX as f64).to_string()),
            (Decimal::from_f64(i64::MIN as f64).unwrap(), (i64::MIN as f64).to_string().into_bytes(), (i64::MIN as f64).to_string()),
            (Decimal::from_f64(u64::MAX as f64).unwrap(), (u64::MAX as f64).to_string().into_bytes(), (u64::MAX as f64).to_string()),
            (Decimal::from_bytes(b"999999999999999999999999").unwrap().unwrap(),
             Vec::from(b"999999999999999999999999".as_ref()),
             String::from("999999999999999999999999")),
        ];

        test_as_string_helper(cs, cast_decimal_as_string, "cast_decimal_as_string");
    }

    #[test]
    fn test_time_as_string() {
        test_none_with_ctx_and_extra(cast_time_as_string);

        // TODO, add more test case
        let cs: Vec<(Time, Vec<u8>, String)> = vec![
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14", 0).unwrap(),
                "2000-01-01 12:13:14".to_string().into_bytes(),
                "2000-01-01 12:13:14".to_string(),
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 0).unwrap(),
                "2000-01-01 12:13:15".to_string().into_bytes(),
                "2000-01-01 12:13:15".to_string()
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 3).unwrap(),
                "2000-01-01 12:13:14.667".to_string().into_bytes(),
                "2000-01-01 12:13:14.667".to_string()
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 4).unwrap(),
                "2000-01-01 12:13:14.6666".to_string().into_bytes(),
                "2000-01-01 12:13:14.6666".to_string()
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 6).unwrap(),
                "2000-01-01 12:13:14.666600".to_string().into_bytes(),
                "2000-01-01 12:13:14.666600".to_string()
            ),
        ];
        test_as_string_helper(cs, cast_time_as_string, "cast_time_as_string");
    }

    #[test]
    fn test_duration_as_string() {
        test_none_with_ctx_and_extra(cast_duration_as_string);

        let cs = vec![
            (Duration::parse(b"17:51:04.78", 2).unwrap(), "17:51:04.78".to_string().into_bytes(), "17:51:04.78".to_string()),
            (Duration::parse(b"-17:51:04.78", 2).unwrap(), "-17:51:04.78".to_string().into_bytes(), "-17:51:04.78".to_string()),
            (Duration::parse(b"17:51:04.78", 0).unwrap(), "17:51:05".to_string().into_bytes(), "17:51:05".to_string()),
            (Duration::parse(b"-17:51:04.78", 0).unwrap(), "-17:51:05".to_string().into_bytes(), "-17:51:05".to_string()),
        ];
        test_as_string_helper(cs, cast_duration_as_string, "cast_duration_as_string");
    }

    #[test]
    fn test_json_as_string() {
        test_none_with_ctx(cast_any_as_any::<Json, Bytes>);

        // FIXME, this case is not exactly same as TiDB's,
        //  such as(left is TiKV, right is TiDB)
        //  f64::MIN =>        "1.7976931348623157e308",  "1.7976931348623157e+308",
        //  f64::MAX =>        "-1.7976931348623157e308", "-1.7976931348623157e+308",
        //  f32::MIN as f64 => "3.4028234663852886e38",   "3.4028234663852886e+38",
        //  f32::MAX as f64 => "-3.4028234663852886e38",  "-3.4028234663852886e+38",
        //  i64::MIN as f64 => "-9.223372036854776e18", "-9223372036854776000",
        //  i64::MAX as f64 => "9.223372036854776e18",  "9223372036854776000",
        //  u64::MAX as f64 => "1.8446744073709552e19", "18446744073709552000",
        let cs = vec![
            (Json::Object(BTreeMap::default()), "{}".to_string()),
            (Json::Array(vec![]), "[]".to_string()),
            (Json::I64(10), "10".to_string()),
            (Json::I64(i64::MAX), i64::MAX.to_string()),
            (Json::I64(i64::MIN), i64::MIN.to_string()),
            (Json::U64(0), "0".to_string()),
            (Json::U64(u64::MAX), u64::MAX.to_string()),
            (Json::Double(f64::MIN), format!("{:e}", f64::MIN)),
            (Json::Double(f64::MAX), format!("{:e}", f64::MAX)),
            (Json::Double(f32::MIN as f64), format!("{:e}", f32::MIN as f64)),
            (Json::Double(f32::MAX as f64), format!("{:e}", f32::MAX as f64)),
            (Json::Double(i64::MIN as f64), format!("{:e}", i64::MIN as f64)),
            (Json::Double(i64::MAX as f64), format!("{:e}", i64::MAX as f64)),
            (Json::Double(u64::MAX as f64), format!("{:e}", u64::MAX as f64)),
            (Json::Double(10.5), "10.5".to_string()),
            (Json::Double(10.4), "10.4".to_string()),
            (Json::Double(-10.4), "-10.4".to_string()),
            (Json::Double(-10.5), "-10.5".to_string()),
            (Json::String(String::from("10.0")), "\"10.0\"".to_string()),
            (Json::Boolean(true), "true".to_string()),
            (Json::Boolean(false), "false".to_string()),
            (Json::None, "null".to_string()),
        ];

        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Json, Bytes>(&mut ctx, &Some(input.clone()));
            let r = r.map(|x| x.map(|x| unsafe { String::from_utf8_unchecked(x) }));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }
}
