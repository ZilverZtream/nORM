using System.Threading;

namespace nORM.Internal;

internal sealed class LockFreeObjectPool<T> where T : class, new()
{
    private readonly ThreadLocal<T?> _threadLocal = new();

    public T Get()
    {
        var item = _threadLocal.Value;
        if (item == null)
        {
            item = new T();
            _threadLocal.Value = item;
        }
        return item;
    }

    /// <summary>
    /// Returns an object to the pool. The instance remains associated with the current
    /// thread via <see cref="ThreadLocal{T}"/> storage and, if it implements
    /// <see cref="IResettable"/>, its state is reset to be reused safely on the next request.
    /// </summary>
    /// <param name="item">The instance to return to the pool.</param>
    public void Return(T item)
    {
        // Item stays in thread-local storage
        if (item is IResettable resettable)
            resettable.Reset();
    }
}
