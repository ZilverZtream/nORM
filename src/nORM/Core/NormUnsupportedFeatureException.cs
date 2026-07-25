using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when a requested feature is not supported by nORM.
    /// </summary>
    public class NormUnsupportedFeatureException : NormException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NormUnsupportedFeatureException"/>.
        /// </summary>
        /// <param name="message">Description of the unsupported feature.</param>
        /// <param name="inner">Optional inner exception.</param>
        public NormUnsupportedFeatureException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance carrying a stable, machine-checkable <see cref="ReasonCode"/> drawn
        /// from <see cref="NormUnsupportedReason"/>. The code identifies WHICH capability limitation was hit,
        /// independently of the human-readable message, so the fuzzing support-contract can distinguish an
        /// intended unsupported shape from a silent capability regression. Internal by design: the code
        /// catalog is not (yet) part of the public API surface — see docs/fuzzing and the reason-code
        /// contract test.
        /// </summary>
        /// <param name="message">Description of the unsupported feature.</param>
        /// <param name="reasonCode">A stable code from <see cref="NormUnsupportedReason"/>.</param>
        /// <param name="inner">Optional inner exception.</param>
        internal NormUnsupportedFeatureException(string message, string reasonCode, Exception? inner = null)
            : base(message, null, null, inner)
        {
            ReasonCode = reasonCode;
        }

        /// <summary>
        /// Stable machine-readable code (from <see cref="NormUnsupportedReason"/>) identifying the specific
        /// capability limitation, or <c>null</c> for a throw-site that has not yet been classified. Internal:
        /// consumed by the test-side support contract via <c>InternalsVisibleTo</c>.
        /// </summary>
        internal string? ReasonCode { get; }
    }
}
