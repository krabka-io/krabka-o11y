use super::*;

/// `count_stream_map_lines` counts entries across every stream, optionally
/// stopping before a timestamp. The bound is EXCLUSIVE, so an entry landing
/// exactly on it is not counted -- that is the one input separating `<`
/// from `<=`, and it matters because the same instant is the next page's
/// first entry and would otherwise be counted twice.
///
/// An entry whose timestamp will not parse IS counted. It is a line that
/// exists, and a count used for paging must not under-report it.
#[test]
pub(crate) fn counting_stream_lines_stops_before_its_bound_but_keeps_odd_entries() {
    let streams = |entries: &[(&str, &[&str])]| {
        entries
            .iter()
            .map(|(app, timestamps)| {
                let mut labels = Labels::default();
                labels.insert("app".to_string(), (*app).to_string());
                (
                    labels,
                    timestamps
                        .iter()
                        .map(|ts| [(*ts).to_string(), "line".to_string()])
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let count = super::super::prelude::count_stream_map_lines;

    // Unbounded: every entry across every stream.
    let two = streams(&[("api", &["1", "2", "3"]), ("web", &["4", "5"])]);
    check!(count(&two, None) == 5, "summed across streams");

    // Bounded, exclusive: 3 is counted, 4 is not.
    check!(count(&two, Some(4)) == 3);
    check!(count(&two, Some(5)) == 4, "the bound itself is excluded");
    check!(count(&two, Some(6)) == 5);
    check!(count(&two, Some(1)) == 0, "nothing before the first");

    // An unparseable timestamp is counted, bounded or not.
    let odd = streams(&[("api", &["1", "nonsense", "9"])]);
    check!(count(&odd, None) == 3);
    check!(count(&odd, Some(2)) == 2, "1 and the odd entry, but not 9");

    // Nothing to count.
    check!(count(&BTreeMap::new(), None) == 0);
    check!(count(&streams(&[("api", &[])]), None) == 0);
}
