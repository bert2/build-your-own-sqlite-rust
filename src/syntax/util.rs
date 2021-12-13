use nom::{character::complete::*, combinator::*, error::*, multi::*, sequence::*, Parser};

pub fn skip<'a, O, E: ParseError<&'a str>, P: Parser<&'a str, O, E>>(
    p: P,
) -> impl Parser<&'a str, (), E> {
    value((), p)
}

pub fn preceded_ws0<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, O, E> {
    preceded(multispace0, f)
}

pub fn preceded_ws1<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, O, E> {
    preceded(multispace1, f)
}

pub fn terminated_ws0<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, O, E> {
    terminated(f, multispace0)
}

pub fn terminated_ws1<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, O, E> {
    terminated(f, multispace1)
}

pub fn delimited_ws0<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, O, E> {
    delimited(multispace0, f, multispace0)
}

pub fn delimited_ws1<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, O, E> {
    delimited(multispace1, f, multispace1)
}

pub fn comma_separated_list1<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    f: F,
) -> impl Parser<&'a str, Vec<O>, E> {
    separated_list1(delimited_ws0(char(',')), f)
}
