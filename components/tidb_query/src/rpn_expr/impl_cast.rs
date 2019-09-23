// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
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
                // if failed, is it because of bug?
                let s: String = box_try!(String::from_utf8(val.to_owned()));
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
    use crate::codec::data_type::{Decimal, Int, Real, ScalarValue};
    use crate::codec::mysql::{Duration, Json, TimeType, Time};
    use crate::expr::Flag;
    use crate::expr::{EvalConfig, EvalContext};
    use crate::rpn_expr::impl_cast::*;
    use crate::rpn_expr::RpnFnCallExtra;
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Display, Formatter};
    use std::sync::Arc;
    use std::{f32, f64, i64, u64};
    use tidb_query_datatype::FieldTypeAccessor;

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

    fn test_none_with_nothing<F, Input, Ret>(func: F)
        where
            F: Fn(&Option<Input>) -> Result<Option<Ret>>,
    {
        let r = func(&None).unwrap();
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

    fn make_ret_field_type_7(parse_to_json: bool) -> FieldType {
        let mut ft = FieldType::default();
        let fta = ft.as_mut_accessor();
        if parse_to_json {
            fta.set_flag(FieldTypeFlag::PARSE_TO_JSON);
        }
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

    fn check_result<R: Debug + PartialEq>(expect: Option<&R>, res: &Result<Option<R>>, log: &str) {
        // if !res.is_ok() {
        //     println!("res.is_ok=false, log: {}", log);
        //     return;
        // }
        assert!(res.is_ok(), "{}", log);
        let res = res.as_ref().unwrap();
        if res.is_none() {
            // if expect.is_some() {
            //     println!("expect should be none, but it is some, log: {}", log);
            // }
            assert!(expect.is_none(), "{}", log);
        } else {
            let res = res.as_ref().unwrap();
            // if expect.unwrap() != res {
            //     println!(
            //         "expect.unwrap()==res: {}, log: {}",
            //         expect.unwrap() == res,
            //         log
            //     );
            // }
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

    #[test]
    fn test_int_as_json() {
        test_none_with_ctx(cast_any_as_any::<Int, Json>);

        let cs = vec![
            (i64::MIN, Json::I64(i64::MIN)),
            (0, Json::I64(0)),
            (i64::MAX, Json::I64(i64::MAX)),
        ];
        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Int, Json>(&mut ctx, &Some(input));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_uint_as_json() {
        test_none_with_nothing(cast_uint_as_json);

        let cs = vec![
            (u64::MAX, Json::U64(u64::MAX)),
            (0, Json::U64(0)),
            (i64::MAX as u64, Json::U64(i64::MAX as u64))
        ];
        for (input, expect) in cs {
            let r = cast_uint_as_json(&Some(input as i64));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_bool_as_json() {
        test_none_with_nothing(cast_bool_as_json);

        let cs = vec![
            (0, Json::Boolean(false)),
            (i64::MIN, Json::Boolean(true)),
            (i64::MAX, Json::Boolean(true)),
        ];
        for (input, expect) in cs {
            let result = cast_bool_as_json(&Some(input));
            let log = make_log(&input, &expect, &result);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_real_as_json() {
        test_none_with_ctx(cast_any_as_any::<Real, Json>);

        let cs = vec![
            (f64::from(f32::MAX), Json::Double(f64::from(f32::MAX))),
            (f64::from(f32::MIN), Json::Double(f64::from(f32::MIN))),
            (f64::MAX, Json::Double(f64::MAX)),
            (f64::MIN, Json::Double(f64::MIN)),
        ];
        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Real, Json>(&mut ctx, &Real::new(input).ok());
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_string_as_json() {
        test_none_with_extra(cast_string_as_json);

        let mut jo1: BTreeMap<String, Json> = BTreeMap::new();
        jo1.insert(String::from("a"), Json::String(String::from("b")));
        // HasParseToJSONFlag
        let cs = vec![
            ("{\"a\": \"b\"}".to_string(), Json::Object(jo1), true),
            ("[1, 2, 3]".to_string(), Json::Array(vec![Json::I64(1), Json::I64(2), Json::I64(3)]), true),
            ("9223372036854775807".to_string(), Json::I64(9223372036854775807), true),
            ("-9223372036854775808".to_string(), Json::I64(-9223372036854775808), true),
            ("18446744073709551615".to_string(), Json::Double(18446744073709552000.0), true),
            // FIXME, f64::MAX.to_string() to json should success
            // (f64::MAX.to_string(), Json::Double(f64::MAX), true),
            ("0.0".to_string(), Json::Double(0.0), true),
            ("\"abcde\"".to_string(), Json::String("abcde".to_string()), true),
            ("\"\"".to_string(), Json::String("".to_string()), true),
            ("true".to_string(), Json::Boolean(true), true),
            ("false".to_string(), Json::Boolean(false), true),
        ];
        for (input, expect, parse_to_json) in cs {
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_7(parse_to_json);
            let extra = make_extra(&rft, &ia);
            let result = cast_string_as_json(&extra, &Some(input.clone().into_bytes()));
            let result_str = result.as_ref().map(|x| x.as_ref().map(|x| x.to_string()));
            let log = format!("input: {}, parse_to_json: {}, expect: {:?}, result: {:?}", input, parse_to_json, expect, result_str);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_decimal_as_json() {
        test_none_with_ctx(cast_any_as_any::<Decimal, Json>);
        let cs = vec![
            (Decimal::from_f64(i64::MIN as f64).unwrap(), Json::Double(i64::MIN as f64)),
            (Decimal::from_f64(i64::MAX as f64).unwrap(), Json::Double(i64::MAX as f64)),
            (Decimal::from_bytes(b"184467440737095516160").unwrap().unwrap(), Json::Double(184467440737095516160.0)),
            (Decimal::from_bytes(b"-184467440737095516160").unwrap().unwrap(), Json::Double(-184467440737095516160.0)),
        ];

        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let r = cast_any_as_any::<Decimal, Json>(&mut ctx, &Some(input.clone()));
            let log = make_log(&input, &expect, &r);
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_time_as_json() {
        test_none_with_ctx(cast_any_as_any::<Time, Json>);

        // TODO, add more case for other TimeType
        let cs = vec![
            // Add time_type filed here is to make maintainer know clearly that what is the type of the time.
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14", 0).unwrap(),
                TimeType::DateTime,
                Json::String("2000-01-01 12:13:14.000000".to_string()),
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 0).unwrap(),
                TimeType::DateTime,
                Json::String("2000-01-01 12:13:15.000000".to_string()),
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14", 6).unwrap(),
                TimeType::DateTime,
                Json::String("2000-01-01 12:13:14.000000".to_string()),
            ),
            (
                Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 6).unwrap(),
                TimeType::DateTime,
                Json::String("2000-01-01 12:13:14.666600".to_string()),
            ),
            (
                Time::parse_utc_datetime("2019-09-01", 0).unwrap(),
                TimeType::DateTime,
                Json::String("2019-09-01 00:00:00.000000".to_string())
            ),
            (
                Time::parse_utc_datetime("2019-09-01", 6).unwrap(),
                TimeType::DateTime,
                Json::String("2019-09-01 00:00:00.000000".to_string())
            ),
        ];
        for (input, time_type, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let result = cast_any_as_any::<Time, Json>(&mut ctx, &Some(input.clone()));
            let result_str = result.as_ref().map(|x| x.as_ref().map(|x| x.to_string()));
            let log = format!(
                "input: {}, expect_time_type: {:?}, real_time_type: {:?}, expect: {}, result: {:?}",
                &input, time_type, input.get_time_type(), &expect, result_str
            );
            // if input.get_time_type() != time_type {
            //     println!("input.get_time_type()==time_type failed, {}", log);
            // }
            assert_eq!(input.get_time_type(), time_type, "{}", log);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_json() {
        test_none_with_ctx(cast_any_as_any::<Duration, Json>);

        // TODO, add more case
        let cs = vec![
            (Duration::zero(), Json::String("00:00:00.000000".to_string())),
            (Duration::parse(b"10:10:10", 0).unwrap(), Json::String("10:10:10.000000".to_string())),
        ];

        for (input, expect) in cs {
            let mut ctx = make_ctx(false, false, false);
            let result = cast_any_as_any::<Duration, Json>(&mut ctx, &Some(input));
            let log = make_log(&input, &expect, &result);
            check_result(Some(&expect), &result, log.as_str());
        }
    }

    #[test]
    fn test_json_as_json() {
        test_none_with_nothing(cast_json_as_json);

        let mut jo1: BTreeMap<String, Json> = BTreeMap::new();
        jo1.insert("a".to_string(), Json::String("b".to_string()));
        let cs = vec![
            Json::Object(jo1),
            Json::Array(vec![Json::I64(1), Json::I64(3), Json::I64(4)]),
            Json::I64(i64::MIN),
            Json::I64(i64::MAX),
            Json::U64(0u64),
            Json::U64(u64::MAX),
            Json::Double(f64::MIN),
            Json::Double(f64::MAX),
            Json::String("abcde".to_string()),
            Json::Boolean(true),
            Json::Boolean(false),
            Json::None,
        ];

        for input in cs {
            let expect = input.clone();
            let result = cast_json_as_json(&Some(input.clone()));
            let log = make_log(&input, &expect, &result);
            check_result(Some(&expect), &result, log.as_str());
        }
    }
}
