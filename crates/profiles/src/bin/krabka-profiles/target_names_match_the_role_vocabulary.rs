use assert2::check;
use clap::ValueEnum as _;

use super::Target;

/// A `--target` an operator types has to be the name the shared vocabulary
/// uses, not merely close to it.
///
/// [`RoleKind::as_str`] is what manifests, runbooks and the other three
/// signals' binaries spell a stage with, and `Target::kind` claims each
/// variant is one of those stages. Nothing but this check ties the two
/// together: clap derives the accepted string from the variant's identifier,
/// so a rename on either side is silent, and the failure -- a deployment whose
/// `-target: block-builder` is rejected by one binary and accepted by three --
/// only shows up when someone tries to start it.
///
/// The strings come back through clap rather than out of the source, because
/// clap's is the one an operator can actually type.
///
/// [`RoleKind::as_str`]: krabka_observability::RoleKind::as_str
#[test]
fn every_target_is_spelled_as_the_shared_vocabulary_spells_it() {
    let variants = Target::value_variants();
    check!(
        variants.len() == 7,
        "every role is offered on the command line"
    );

    for target in variants {
        let accepted = target
            .to_possible_value()
            .expect("every target is selectable on the command line");
        check!(
            accepted.get_name() == target.kind().as_str(),
            "--target {} is spelled {} by the shared vocabulary",
            accepted.get_name(),
            target.kind().as_str()
        );
    }
}
