using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;

namespace nORM.Query
{
    /// <summary>
    /// Manages SQL parameters, compiled parameter names and mapping between
    /// expression parameters and generated SQL parameter placeholders.
    /// Uses a small preallocated array to avoid dictionary allocations for
    /// common query scenarios with few parameters.
    /// </summary>
    internal sealed class ParameterManager
    {
        public IDictionary<string, object> Parameters { get; set; } = new FastParameterCollection();
        public List<string> CompiledParameters { get; set; } = new();
        public Dictionary<ParameterExpression, string> ParameterMap { get; set; } = new();

        private int _index;

        public int Index
        {
            get => Volatile.Read(ref _index);
            set => Volatile.Write(ref _index, value);
        }

        public int GetNextIndex() => Interlocked.Increment(ref _index) - 1;

        public void Reset()
        {
            Parameters = new FastParameterCollection();
            CompiledParameters = new List<string>();
            ParameterMap = new Dictionary<ParameterExpression, string>();
            Volatile.Write(ref _index, 0);
        }

        /// <summary>
        /// Dictionary-like collection optimized for up to 32 parameters before
        /// falling back to a standard <see cref="Dictionary{TKey,TValue}"/>.
        /// </summary>
        internal sealed class FastParameterCollection : IDictionary<string, object>, IReadOnlyDictionary<string, object>
        {
            private const int InlineCapacity = 32;
            private readonly object[] _values = new object[InlineCapacity];
            private readonly string[] _names = new string[InlineCapacity];
            private int _count;
            private Dictionary<string, object>? _overflow;

            private Dictionary<string, object> EnsureOverflow()
            {
                if (_overflow == null)
                {
                    _overflow = new Dictionary<string, object>(InlineCapacity);
                    for (var i = 0; i < _count; i++)
                    {
                        _overflow[_names[i]] = _values[i];
                        _names[i] = null!;
                        _values[i] = null!;
                    }
                    _count = 0;
                }

                return _overflow;
            }

            public object this[string key]
            {
                get
                {
                    if (_overflow != null)
                        return _overflow[key];

                    for (var i = 0; i < _count; i++)
                        if (_names[i] == key)
                            return _values[i];

                    throw new KeyNotFoundException();
                }
                set
                {
                    if (_overflow != null)
                    {
                        _overflow[key] = value;
                        return;
                    }

                    for (var i = 0; i < _count; i++)
                    {
                        if (_names[i] == key)
                        {
                            _values[i] = value;
                            return;
                        }
                    }

                    if (_count < InlineCapacity)
                    {
                        _names[_count] = key;
                        _values[_count] = value;
                        _count++;
                    }
                    else
                    {
                        EnsureOverflow()[key] = value;
                    }
                }
            }

            ICollection<string> IDictionary<string, object>.Keys
                => _overflow != null ? (ICollection<string>)_overflow.Keys : GetNames();

            ICollection<object> IDictionary<string, object>.Values
                => _overflow != null ? (ICollection<object>)_overflow.Values : GetValues();

            IEnumerable<string> IReadOnlyDictionary<string, object>.Keys
                => _overflow != null ? (IEnumerable<string>)_overflow.Keys : EnumerateNames();

            IEnumerable<object> IReadOnlyDictionary<string, object>.Values
                => _overflow != null ? (IEnumerable<object>)_overflow.Values : EnumerateValues();

            public int Count => _overflow?.Count ?? _count;

            public bool IsReadOnly => false;

            public void Add(string key, object value)
            {
                if (_overflow != null)
                {
                    _overflow.Add(key, value);
                    return;
                }

                for (var i = 0; i < _count; i++)
                    if (_names[i] == key)
                        throw new ArgumentException("An item with the same key has already been added.");

                if (_count < InlineCapacity)
                {
                    _names[_count] = key;
                    _values[_count] = value;
                    _count++;
                }
                else
                {
                    EnsureOverflow().Add(key, value);
                }
            }

            public bool ContainsKey(string key)
            {
                if (_overflow != null)
                    return _overflow.ContainsKey(key);

                for (var i = 0; i < _count; i++)
                    if (_names[i] == key)
                        return true;

                return false;
            }

            public bool Remove(string key)
            {
                if (_overflow != null)
                    return _overflow.Remove(key);

                for (var i = 0; i < _count; i++)
                {
                    if (_names[i] == key)
                    {
                        _count--;
                        if (i < _count)
                        {
                            _names[i] = _names[_count];
                            _values[i] = _values[_count];
                        }
                        _names[_count] = null!;
                        _values[_count] = null!;
                        return true;
                    }
                }
                return false;
            }

            public bool TryGetValue(string key, out object value)
            {
                if (_overflow != null)
                    return _overflow.TryGetValue(key, out value!);

                for (var i = 0; i < _count; i++)
                {
                    if (_names[i] == key)
                    {
                        value = _values[i];
                        return true;
                    }
                }
                value = null!;
                return false;
            }

            public void Add(KeyValuePair<string, object> item) => Add(item.Key, item.Value);

            public void Clear()
            {
                if (_overflow != null)
                {
                    _overflow.Clear();
                    _overflow = null;
                }
                for (var i = 0; i < _count; i++)
                {
                    _names[i] = null!;
                    _values[i] = null!;
                }
                _count = 0;
            }

            public bool Contains(KeyValuePair<string, object> item)
            {
                if (_overflow != null)
                    return ((IDictionary<string, object>)_overflow).Contains(item);

                for (var i = 0; i < _count; i++)
                    if (_names[i] == item.Key && Equals(_values[i], item.Value))
                        return true;
                return false;
            }

            public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
            {
                if (_overflow != null)
                {
                    ((IDictionary<string, object>)_overflow).CopyTo(array, arrayIndex);
                    return;
                }

                for (var i = 0; i < _count; i++)
                    array[arrayIndex + i] = new KeyValuePair<string, object>(_names[i], _values[i]);
            }

            public bool Remove(KeyValuePair<string, object> item)
            {
                if (_overflow != null)
                    return ((IDictionary<string, object>)_overflow).Remove(item);

                for (var i = 0; i < _count; i++)
                {
                    if (_names[i] == item.Key && Equals(_values[i], item.Value))
                    {
                        Remove(item.Key);
                        return true;
                    }
                }
                return false;
            }

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
                => (_overflow != null ? _overflow : EnumerateInline()).GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            private IEnumerable<KeyValuePair<string, object>> EnumerateInline()
            {
                for (var i = 0; i < _count; i++)
                    yield return new KeyValuePair<string, object>(_names[i], _values[i]);
            }

            private List<string> GetNames()
            {
                var list = new List<string>(_count);
                for (var i = 0; i < _count; i++)
                    list.Add(_names[i]);
                return list;
            }

            private List<object> GetValues()
            {
                var list = new List<object>(_count);
                for (var i = 0; i < _count; i++)
                    list.Add(_values[i]);
                return list;
            }

            private IEnumerable<string> EnumerateNames()
            {
                for (var i = 0; i < _count; i++)
                    yield return _names[i];
            }

            private IEnumerable<object> EnumerateValues()
            {
                for (var i = 0; i < _count; i++)
                    yield return _values[i];
            }
        }
    }
}

