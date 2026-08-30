use super::*;

/// `page_groups` pages the rules response by group, resuming AFTER the
/// token the client sent rather than at it -- resuming at it would return
/// the same group forever. The token it hands back names the LAST group in
/// the page, which is what makes the next request resume correctly.
///
/// The two are checked against each other by walking a five-group list to
/// exhaustion in pages of two: an off-by-one in either the resume or the
/// token shows up as a repeated or skipped group rather than as a wrong
/// count.
#[test]
pub(crate) fn paging_rule_groups_resumes_after_the_token_it_handed_back() {
    let groups = || {
        ["a", "b", "c", "d", "e"]
            .iter()
            .map(|name| super::super::prelude::PrometheusRuleGroupResponse {
                token: (*name).to_string(),
                value: serde_json::json!({"name": name}),
            })
            .collect::<Vec<_>>()
    };
    let page = |limit: Option<usize>, token: Option<&str>| {
        super::super::prelude::PrometheusRulesFilters {
            group_limit: limit,
            group_next_token: token.map(str::to_string),
            ..super::super::prelude::PrometheusRulesFilters::default()
        }
        .page_groups(groups())
    };
    let names = |page: &super::super::prelude::PrometheusRulesPage| {
        page.groups
            .iter()
            .map(|group| group["name"].as_str().expect("a name").to_string())
            .collect::<Vec<_>>()
    };

    // No limit returns everything, with nothing to resume from.
    let all = page(None, None).expect("no limit is valid");
    check!(names(&all) == vec!["a", "b", "c", "d", "e"]);
    check!(all.next_token.is_none());

    // Walk the list in pages of two. The token names the last group
    // returned, and the next page starts after it.
    let first = page(Some(2), None).expect("a first page");
    check!(names(&first) == vec!["a", "b"]);
    check!(
        first.next_token.as_deref() == Some("b"),
        "the LAST group of the page"
    );

    let second = page(Some(2), Some("b")).expect("a second page");
    check!(
        names(&second) == vec!["c", "d"],
        "resumes after b, not at it"
    );
    check!(second.next_token.as_deref() == Some("d"));

    // The final page is short and offers no token, because nothing follows.
    let third = page(Some(2), Some("d")).expect("a third page");
    check!(names(&third) == vec!["e"]);
    check!(third.next_token.is_none());

    // A page that exactly exhausts the list offers no token either: the
    // boundary is `>` and not `>=`, or a client would ask for an empty page.
    let exact = page(Some(5), None).expect("an exact page");
    check!(names(&exact) == vec!["a", "b", "c", "d", "e"]);
    check!(exact.next_token.is_none(), "nothing follows an exact fit");

    // A zero limit returns nothing and offers no token to resume from,
    // rather than a token that would never advance.
    let none = page(Some(0), None).expect("a zero limit is valid");
    check!(names(&none).is_empty());
    check!(none.next_token.is_none());

    // Resuming from the last group leaves an empty page.
    let past = page(Some(2), Some("e")).expect("resuming from the end");
    check!(names(&past).is_empty());

    // A token naming no group is a client error, not an empty page: it
    // usually means the group was deleted between requests.
    check!(page(Some(2), Some("nonsense")).is_err());
}
