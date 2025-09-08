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

    public void Return(T item)
    {
        // Item stays in thread-local storage
        if (item is IResettable resettable)
            resettable.Reset();
    }
}
