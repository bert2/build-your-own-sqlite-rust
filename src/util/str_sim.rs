use std::{
    cmp::{max, min},
    collections::hash_set::HashSet,
};

pub fn similarity(a: &str, b: &str) -> f32 {
    if a.is_empty() && b.is_empty() {
        return 1.;
    }

    let max_len = max(a.len(), b.len()) as f32;
    let min_len = min(a.len(), b.len()) as f32;

    let num_matches = a
        .chars()
        .enumerate()
        .collect::<HashSet<_>>()
        .intersection(&b.chars().enumerate().collect())
        .count();
    let match_rate = (num_matches as f32) / max_len;

    let len_sim = min_len / max_len;

    let match_weight = 2. / 3.;
    let len_weight = 1. / 3.;
    match_rate * match_weight + len_sim * len_weight
}

pub fn most_similar<'a, I>(target: &str, candidates: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    candidates
        .into_iter()
        .map(|c| (c, similarity(target, c)))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|max| max.0)
}

#[cfg(test)]
mod test {
    mod similarity_tests {
        use super::super::similarity;

        #[test]
        fn is_1_for_equal_inputs() {
            assert_eq!(similarity("foo", "foo"), 1.);
            assert_eq!(similarity("bar", "bar"), 1.);
        }

        #[test]
        fn is_less_than_1_for_unequal_inputs() {
            assert!(similarity("foo", "bar") < 1.);
            assert!(similarity("bar", "qux") < 1.);
        }

        #[test]
        fn is_higher_when_more_chars_match() {
            assert!(similarity("foo", "foo") > similarity("foo", "foA"));
            assert!(similarity("foo", "foA") > similarity("foo", "fAA"));
            assert!(similarity("foo", "fAA") > similarity("foo", "AAA"));
        }

        #[test]
        fn is_higher_when_lengths_are_closer() {
            assert!(similarity("foo", "foo") > similarity("foo", "fooo"));
            assert!(similarity("foo", "fooo") > similarity("foo", "foooo"));
            assert!(similarity("foo", "foooo") > similarity("foo", "fooooo"));
        }

        #[test]
        fn prefers_matching_length_over_char_matches() {
            assert!(similarity("foo", "foA") > similarity("foo", "fooo"));
            assert!(similarity("foo", "foA") > similarity("fo", "fooo"));
        }

        #[test]
        fn is_case_sensitive() {
            assert!(similarity("foo", "FOO") < 1.);
            assert!(similarity("bar", "baR") < 1.);
        }

        #[test]
        fn empty_inputs_dont_return_nan() {
            assert_eq!(similarity("foo", ""), 0.);
            assert_eq!(similarity("", "foo"), 0.);
            assert_eq!(similarity("", ""), 1.);
        }

        #[test]
        fn is_commutative() {
            assert_eq!(similarity("foo", "bar"), similarity("bar", "foo"));
            assert_eq!(similarity("foo", "foobar"), similarity("foobar", "foo"));
        }
    }

    mod most_similar_tests {
        use super::super::most_similar;

        #[test]
        fn finds_closest_match() {
            assert_eq!(
                most_similar("if", vec!["id", "name", "color"].into_iter()),
                Some("id")
            );
            assert_eq!(
                most_similar("mame", vec!["id", "name", "color"].into_iter()),
                Some("name")
            );
            assert_eq!(
                most_similar("rotor", vec!["id", "name", "color"].into_iter()),
                Some("color")
            );
        }
    }
}
