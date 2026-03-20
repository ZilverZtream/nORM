using System;
#nullable enable
namespace nORM.Mapping
{
    /// <summary>Defines a bi-directional conversion between a model type and a provider (database) type.</summary>
    public interface IValueConverter
    {
        /// <summary>Gets the CLR type of the model-side value.</summary>
        Type ModelType    { get; }
        /// <summary>Gets the CLR type of the provider/database-side value.</summary>
        Type ProviderType { get; }
        /// <summary>Converts a model value (possibly null) to the provider representation.</summary>
        object? ConvertToProvider(object? modelValue);
        /// <summary>Converts a provider value (possibly null) back to the model representation.</summary>
        object? ConvertFromProvider(object? providerValue);
    }

    /// <summary>
    /// Strongly-typed base class for value converters. Override
    /// <see cref="ConvertToProvider(TModel)"/> and <see cref="ConvertFromProvider(TProvider)"/>.
    /// Null values pass through the explicit interface without invoking the override.
    /// </summary>
    /// <typeparam name="TModel">The CLR model type.</typeparam>
    /// <typeparam name="TProvider">The database/provider storage type.</typeparam>
    public abstract class ValueConverter<TModel, TProvider> : IValueConverter
    {
        /// <inheritdoc/>
        public Type ModelType    => typeof(TModel);
        /// <inheritdoc/>
        public Type ProviderType => typeof(TProvider);

        /// <summary>
        /// Converts a non-null model value to the provider type.
        /// Return null to store a database NULL.
        /// </summary>
        public abstract object? ConvertToProvider(TModel value);

        /// <summary>
        /// Converts a non-null provider value back to the model type.
        /// Return null when the model value should be null.
        /// </summary>
        public abstract object? ConvertFromProvider(TProvider value);

        object? IValueConverter.ConvertToProvider(object? modelValue)
        {
            if (modelValue == null || modelValue is DBNull) return null;
            if (modelValue is TModel typed) return ConvertToProvider(typed);
            // Cross-type conversion (e.g., boxed int when TModel=long)
            try
            {
                return ConvertToProvider((TModel)System.Convert.ChangeType(modelValue, typeof(TModel)));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException(
                    $"Cannot convert model value of type '{modelValue.GetType().Name}' to '{typeof(TModel).Name}' " +
                    $"for converter '{GetType().Name}'.", ex);
            }
        }

        object? IValueConverter.ConvertFromProvider(object? providerValue)
        {
            if (providerValue == null || providerValue is DBNull) return null;
            if (providerValue is TProvider typed) return ConvertFromProvider(typed);
            // Cross-type conversion (e.g., SQLite returns long for INTEGER, TProvider=int)
            try
            {
                return ConvertFromProvider((TProvider)System.Convert.ChangeType(providerValue, typeof(TProvider)));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException(
                    $"Cannot convert provider value of type '{providerValue.GetType().Name}' to '{typeof(TProvider).Name}' " +
                    $"for converter '{GetType().Name}'.", ex);
            }
        }
    }
}
