using System.Diagnostics.CodeAnalysis;

#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Represents the result of an interceptor call. When <see cref="IsSuppressed"/> is true,
    /// the command execution is skipped and <see cref="Result"/> is returned instead.
    /// </summary>
    /// <typeparam name="T">Type of the result.</typeparam>
    public readonly struct InterceptionResult<T>
    {
        public bool IsSuppressed { get; }
        [MaybeNull]
        public T Result { get; }

        private InterceptionResult(bool suppressed, T result)
        {
            IsSuppressed = suppressed;
            Result = result;
        }

        public static InterceptionResult<T> Continue() => default;

        public static InterceptionResult<T> SuppressWithResult(T result) => new(true, result);
    }
}
