using System;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// The outcome taxonomy every fuzz case is classified into. A green run must mean "correct AND still
    /// capable", not merely "never silently wrong" — so a clean rejection is only healthy when it is a
    /// <em>documented</em> rejection (<see cref="CorrectlyRejected"/>); a rejection with no registered reason
    /// code (<see cref="UnexpectedlyRejected"/>) signals silently declining coverage and fails the run.
    /// </summary>
    /// <remarks>
    /// Members are ordered by ascending severity so the worst outcome of a set sorts last, and a run can
    /// report its worst outcome by taking the maximum. <see cref="Executed"/> and <see cref="CorrectlyRejected"/>
    /// are the only healthy outcomes; every other value is a failure (<see cref="FuzzOutcomeExtensions.IsFailure"/>).
    /// </remarks>
    public enum FuzzOutcome
    {
        /// <summary>Ran to completion and agreed with every oracle. Healthy.</summary>
        Executed = 0,

        /// <summary>
        /// Threw a documented <c>NormUnsupportedFeatureException</c> carrying a reason code registered in the
        /// support contract. Fail-loud on an unsupported shape is correct behavior — healthy, and counted so
        /// the unsupported rate can be gated against a baseline.
        /// </summary>
        CorrectlyRejected = 1,

        /// <summary>
        /// Threw an unsupported-feature signal whose reason code is NOT in the checked-in support contract.
        /// Either the generator reached a shape nORM newly refuses, or a throw-site lacks a registered code.
        /// This is how "unsupported" conceals declining coverage; it fails the run until the contract is
        /// updated with review.
        /// </summary>
        UnexpectedlyRejected = 2,

        /// <summary>A per-case limit (translation/execution time, SQL length, cardinality, memory) was exceeded — a hang or resource blowup. Failure.</summary>
        Timeout = 3,

        /// <summary>Threw an exception that is not a documented unsupported-feature signal (e.g. NullReference, argument, provider error on a shape that should translate). Failure.</summary>
        UnexpectedException = 4,

        /// <summary>Two executions of the SAME case disagreed. A non-deterministic result is never acceptable. Failure.</summary>
        NonDeterministic = 5,

        /// <summary>Executed but disagreed with an oracle: the cardinal sin — a silently wrong result. The highest-severity failure.</summary>
        WrongResult = 6,
    }

    /// <summary>Helpers over <see cref="FuzzOutcome"/>.</summary>
    public static class FuzzOutcomeExtensions
    {
        /// <summary>
        /// True when the outcome should fail the run. Only <see cref="FuzzOutcome.Executed"/> and
        /// <see cref="FuzzOutcome.CorrectlyRejected"/> are healthy; everything else — including an
        /// unregistered rejection — is a failure.
        /// </summary>
        public static bool IsFailure(this FuzzOutcome outcome)
            => outcome is not (FuzzOutcome.Executed or FuzzOutcome.CorrectlyRejected);

        /// <summary>True when the outcome is a silent-correctness defect (wrong or non-deterministic result) — the worst class.</summary>
        public static bool IsSilentDefect(this FuzzOutcome outcome)
            => outcome is FuzzOutcome.WrongResult or FuzzOutcome.NonDeterministic;
    }
}
