using System;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// A node visited during <see cref="ChangeTracker.TrackGraph(object, Action{EntityEntryGraphNode})"/>:
    /// the entry for the entity currently being processed, the entry it was reached from, and the navigation
    /// property that was traversed to reach it. The callback assigns <see cref="EntityEntry.State"/> on
    /// <see cref="Entry"/> to choose how the entity is tracked; leaving it <see cref="EntityState.Detached"/>
    /// stops the traversal from descending into that entity's own navigations (Entity Framework Core parity).
    /// </summary>
    public sealed class EntityEntryGraphNode
    {
        internal EntityEntryGraphNode(EntityEntry entry, EntityEntry? sourceEntry, string? inboundNavigation)
        {
            Entry = entry;
            SourceEntry = sourceEntry;
            InboundNavigation = inboundNavigation;
        }

        /// <summary>
        /// The entry for the entity currently being visited. Set its <see cref="EntityEntry.State"/> to
        /// begin tracking the entity in the chosen state.
        /// </summary>
        public EntityEntry Entry { get; }

        /// <summary>
        /// The entry of the entity from which this node was reached, or <c>null</c> when this node is the
        /// root passed to <see cref="ChangeTracker.TrackGraph(object, Action{EntityEntryGraphNode})"/>.
        /// </summary>
        public EntityEntry? SourceEntry { get; }

        /// <summary>
        /// The name of the navigation property traversed from <see cref="SourceEntry"/> to reach this node,
        /// or <c>null</c> for the root.
        /// </summary>
        public string? InboundNavigation { get; }
    }
}
