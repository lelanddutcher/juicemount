#!/usr/bin/env python3
"""Self-test for the harness comparison rules. Exits non-zero on any failure.

These are the rules that decide whether a performance gate is trustworthy, so
they get tested directly rather than only through a harness that uses them.
"""

import sys

import harness as H


def doc(samples, cls="fast", measured=True):
    return {
        "stats": H.stats(samples),
        "envelope": {"link": {"class": cls, "class_measured": measured}},
    }


FAILS = []


def check(name, got, want):
    if got != want:
        FAILS.append("%s: got %s, want %s" % (name, got, want))
    print("  %-52s %s" % (name, got))


def main():
    six = [10, 10.1, 9.9, 10.2, 9.8, 10.0]

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
