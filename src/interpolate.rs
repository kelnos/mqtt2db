use lazy_static::lazy_static;
use regex::Regex;
use std::convert::TryFrom;

lazy_static! {
    static ref REFERENCE_RE: Regex = Regex::new(r"(^|[^\\])(\$(\d+))").unwrap();
}

#[derive(Clone, Debug, PartialEq)]
pub struct InterpolatedName {
    pub parts: Vec<InterpolatedNamePart>,
    n_references: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum InterpolatedNamePart {
    Literal(String),
    Reference(usize),
}

impl TryFrom<&str> for InterpolatedName {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> Result<InterpolatedName, Self::Error> {
        let mut parts: Vec<InterpolatedNamePart> = Vec::new();
        let mut n_references: usize = 0;
        let mut pos: usize = 0;

        for cap in REFERENCE_RE.captures_iter(s) {
            let mat = cap
                .get(2)
                .ok_or_else(|| anyhow!("Unable to get full match for name '{}'", s))?;
            if pos < mat.start() {
                parts.push(InterpolatedNamePart::Literal(
                    s.chars()
                        .skip(pos)
                        .take(mat.start() - pos)
                        .collect::<String>()
                        .replace("\\", ""),
                ));
            }

            let num_str = cap
                .get(3)
                .map(|mat1| mat1.as_str())
                .ok_or_else(|| anyhow!("Unable to get capture group for name '{}'", s))?;
            let num = num_str
                .parse::<usize>()
                .map_err(|_| anyhow!("Couldn't parse '{}' as number for name '{}'", num_str, s))?;
            if num == 0 {
                Err(anyhow!("Invalid reference number 0 for name '{}'", s))?;
            }
            parts.push(InterpolatedNamePart::Reference(num));
            n_references += 1;

            pos = mat.end();
        }

        if pos < s.len() {
            parts.push(InterpolatedNamePart::Literal(
                s.chars().skip(pos).collect::<String>().replace("\\", ""),
            ));
        }

        Ok(InterpolatedName {
            parts,
            n_references,
        })
    }
}

impl InterpolatedName {
    pub fn interpolate<S: AsRef<str>>(&self, reference_values: &Vec<S>) -> anyhow::Result<String> {
        self.parts
            .iter()
            .fold(Ok(String::new()), |accum, part| match accum {
                Ok(mut accum) => match part {
                    InterpolatedNamePart::Literal(s) => {
                        accum.push_str(s.as_str());
                        Ok(accum)
                    }
                    InterpolatedNamePart::Reference(num) => match reference_values.get(*num - 1) {
                        Some(reference_value) => {
                            accum.push_str(reference_value.as_ref());
                            Ok(accum)
                        }
                        None => Err(anyhow!(
                            "Can't find reference number {} to interpolate",
                            num
                        )),
                    },
                },
                Err(err) => Err(err),
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn interpolated_name_parsing() -> anyhow::Result<()> {
        use InterpolatedNamePart::*;

        assert_eq!(
            vec![Literal("foo".to_string())],
            InterpolatedName::try_from("foo")?.parts
        );

        assert_eq!(vec![Reference(1)], InterpolatedName::try_from("$1")?.parts);

        assert_eq!(
            vec![
                Literal("foo".to_string()),
                Reference(1),
                Literal("bar".to_string()),
                Reference(2),
                Literal(" baz".to_string())
            ],
            InterpolatedName::try_from("foo$1bar$2 baz")?.parts
        );

        assert_eq!(
            vec![
                Literal("$1foo".to_string()),
                Reference(1),
                Literal("$2".to_string())
            ],
            InterpolatedName::try_from("\\$1foo$1\\$2")?.parts
        );

        assert!(InterpolatedName::try_from("$0").is_err());

        Ok(())
    }

    #[test]
    fn interpolation() -> anyhow::Result<()> {
        let interp = InterpolatedName::try_from("foo$1bar$2 baz $1")?;
        assert_eq!(
            "foofirstbarsecond baz first".to_string(),
            interp
                .interpolate(&vec!["first".to_string(), "second".to_string()])
                .unwrap()
        );

        let empty: Vec<String> = vec![];
        assert!(interp.interpolate(&empty).is_err());

        Ok(())
    }
}
