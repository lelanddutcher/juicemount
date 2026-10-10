#!/usr/bin/env python3
"""Self-test for the harness comparison rules. Exits non-zero on any failure.

These are the rules that decide whether a performance gate is trustworthy, so
they get tested directly rather than only through a harness that uses them.
"""

import sys

import harness as H


def doc(samples, cls="fast", measured=True, simulated=False):
    return {
        "stats": H.stats(samples),
        "envelope": {"link": {"class": cls, "class_measured": measured,
                              "simulated": simulated}},
    }


FAILS = []


def check(name, got, want):
    if got != want:
        FAILS.append("%s: got %s, want %s" % (name, got, want))
    print("  %-52s %s" % (name, got))


def main():
    six = [10, 10.1, 9.9, 10.2, 9.8, 10.0]

    # A synthetic profile is stamped verbatim from the canonical /metrics
    # field, but can never claim its synthetic RTT/bandwidth as measured.
    original_metrics = H._metrics
    try:
        H._metrics = lambda: {"network": {
            "class": "metered", "have_rtt": True, "have_bandwidth": True,
            "simulated": True,
            "fake_profile": "rtt_ms=300,bw_down_bps=2000000,bw_up_bps=1000000,loss_pct=0.5,class=metered",
        }}
        synthetic_link = H.envelope()["link"]
    finally:
        H._metrics = original_metrics
    check("synthetic envelope is stamped simulated",
          synthetic_link["simulated"], True)
    check("synthetic class is NOT measured",
          synthetic_link["class_measured"], False)
    check("synthetic envelope carries canonical profile",
          synthetic_link["fake_profile"],
          "rtt_ms=300,bw_down_bps=2000000,bw_up_bps=1000000,loss_pct=0.5,class=metered")

    # THE ANTI-NOISE RULE. TestBenchmarkSuite fails at 1.20x while varying
    # 2.2-4.4x run to run. Overlapping ranges mean the difference is not
    # separable from noise, and calling that a regression is what taught
    # everyone to ignore the gate.
    noisy = [10, 24, 15, 31, 12, 44]
    check("overlapping ranges -> PASS (not separable from noise)",
          H.compare(doc(six), doc(noisy))["verdict"], H.PASS)

    # A real regression: disjoint ranges, worse median.
    check("disjoint + worse -> FAIL",
          H.compare(doc(six), doc([30, 31, 32, 30.5, 31.5, 33]))["verdict"], H.FAIL)

    # Disjoint but BETTER is a pass, not a failure.
    check("disjoint + better -> PASS",
          H.compare(doc(six), doc([2, 2.1, 1.9, 2.2, 1.8, 2.0]))["verdict"], H.PASS)

    # Not enough samples to have an opinion. Must NOT be a pass.
    check("n<6 -> INVALID (not a pass)",
          H.compare(doc([1, 2, 3]), doc(six))["verdict"], H.INVALID)

    # THE CROSS-CLASS TRAP. Comparing a cellular run to a LAN baseline yields a
    # number, and the number is meaningless.
    check("LAN baseline vs cellular run -> INCOMPARABLE",
          H.compare(doc(six, "fast"), doc(six, "metered"))["verdict"], H.INCOMPARABLE)

    # netprofile reports a default class before it has samples. A class that was
    # assumed is not a class that was measured.
    check("class not MEASURED -> INCOMPARABLE",
          H.compare(doc(six, "fast", True), doc(six, "fast", False))["verdict"],
          H.INCOMPARABLE)

    # ...unless the class IS the experiment. Pinning JM_NET_FORCE_CLASS to
    # measure what the cellular mitigation buys is a DECLARED cross-class A/B,
    # and refusing to judge it would make the product's whole answer to
    # "be efficient on cellular" permanently unmeasurable.
    check("declared link_class treatment -> judged, not refused",
          H.compare(doc(six, "fast"), doc([2, 2.1, 1.9, 2.2, 1.8, 2.0], "metered"),
                    treatment="link_class")["verdict"], H.PASS)

    # The declaration must not become a blanket escape hatch: it waives the
    # class-EQUALITY guard only. An unmeasured class is still an assumption, and
    # an assumption is still incomparable.
    check("treatment does NOT waive the class-measured guard",
          H.compare(doc(six, "fast", True), doc(six, "metered", False),
                    treatment="link_class")["verdict"], H.INCOMPARABLE)

    # And it must not waive the sample-count floor either.
    check("treatment does NOT waive n>=6",
          H.compare(doc([1, 2, 3], "fast"), doc(six, "metered"),
                    treatment="link_class")["verdict"], H.INVALID)

    # FakeLink is decision-level policy simulation. It replaces observations
    # but does not shape packets, so even two identical synthetic profiles must
    # not be mistaken for measured cellular timing.
    check("simulated vs real -> INCOMPARABLE",
          H.compare(doc(six), doc(six, "metered", False, True))["verdict"],
          H.INCOMPARABLE)
    check("simulated vs simulated -> INCOMPARABLE",
          H.compare(doc(six, "metered", False, True),
                    doc(six, "metered", False, True),
                    treatment="link_class")["verdict"], H.INCOMPARABLE)

    # A treatment nobody declared is still the trap.
    check("unrecognised treatment name -> still INCOMPARABLE",
          H.compare(doc(six, "fast"), doc(six, "metered"),
                    treatment="something_else")["verdict"], H.INCOMPARABLE)

    # Same class, same measured-ness, identical data: the trivial pass.
    check("identical runs -> PASS",
          H.compare(doc(six), doc(six))["verdict"], H.PASS)

    print()
    if FAILS:
        print("FAILURES:")
        for f in FAILS:
            print("  " + f)
        return 1
    print("all harness comparison rules hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
