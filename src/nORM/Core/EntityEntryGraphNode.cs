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

    /// <summary>
    /// A node visited during the stateful
    /// <see cref="ChangeTracker.TrackGraph{TState}(object, TState, System.Func{EntityEntryGraphNode{TState}, bool})"/>,
    /// carrying the caller's <typeparamref name="TState"/> alongside the entry, its source, and the inbound
    /// navigation (Entity Framework Core parity). The callback assigns <see cref="EntityEntry.State"/> on
    /// <see cref="Entry"/> to track the entity, and returns whether the traversal should descend into this
    /// entity's own navigations.
    /// </summary>
    /// <typeparam name="TState">Type of the state value threaded through the traversal.</typeparam>
    public sealed class EntityEntryGraphNode<TState>
    {
        internal EntityEntryGraphNode(EntityEntry entry, EntityEntry? sourceEntry, string? inboundNavigation, TState nodeState)
        {
            Entry = entry;
            SourceEntry = sourceEntry;
            InboundNavigation = inboundNavigation;
            NodeState = nodeState;
        }

        /// <summary>
        /// The entry for the entity currently being visited. Set its <see cref="EntityEntry.State"/> to
        /// begin tracking the entity in the chosen state.
        /// </summary>
        public EntityEntry Entry { get; }

        /// <summary>The entry this node was reached from, or <c>null</c> when this node is the root.</summary>
        public EntityEntry? SourceEntry { get; }

        /// <summary>The navigation traversed from <see cref="SourceEntry"/> to reach this node, or <c>null</c> for the root.</summary>
        public string? InboundNavigation { get; }

        /// <summary>The state value threaded through the traversal from the TrackGraph call.</summary>
        public TState NodeState { get; }
    }
}
