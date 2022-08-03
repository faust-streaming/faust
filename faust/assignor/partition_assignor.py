"""Partition assignor."""
import socket
import zlib
from collections import defaultdict
from typing import Iterable, List, Mapping, MutableMapping, Sequence, Set, cast

from kafka.cluster import ClusterMetadata
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)
from mode import get_logger
from yarl import URL

from faust.types.app import AppT
from faust.types.assignor import (
    HostToPartitionMap,
    PartitionAssignorT,
    TopicToPartitionMap,
)
from faust.types.tables import TableManagerT
from faust.types.tuples import TP

from .client_assignment import ClientAssignment, ClientMetadata
from .cluster_assignment import ClusterAssignment
from .copartitioned_assignor import CopartitionedAssignor

__all__ = [
    "MemberAssignmentMapping",
    "MemberMetadataMapping",
    "MemberSubscriptionMapping",
    "ClientMetadataMapping",
    "ClientAssignmentMapping",
    "CopartitionedGroups",
    "PartitionAssignor",
]

MemberAssignmentMapping = MutableMapping[str, ConsumerProtocolMemberAssignment]
MemberMetadataMapping = MutableMapping[str, ConsumerProtocolMemberMetadata]
MemberSubscriptionMapping = MutableMapping[str, List[str]]
ClientMetadataMapping = MutableMapping[str, ClientMetadata]
ClientAssignmentMapping = MutableMapping[str, ClientAssignment]
CopartitionedGroups = MutableMapping[int, Iterable[Set[str]]]

logger = get_logger(__name__)


class PartitionAssignor(AbstractPartitionAssignor, PartitionAssignorT):  # type: ignore
    """PartitionAssignor handles internal topic creation.

    Further, this assignor needs to be sticky and potentially redundant
    In addition, it tracks external topic assignments as well (to support topic routes)

    Notes:
        Interface copied from :mod:`kafka.coordinator.assignors.abstract`.
    """

    _assignment: ClientAssignment
    _table_manager: TableManagerT
    _member_urls: MutableMapping[str, str]
    _changelog_distribution: HostToPartitionMap
    _external_topic_distribution: HostToPartitionMap
    _active_tps: Set[TP]
    _standby_tps: Set[TP]
    _tps_url: MutableMapping[TP, str]
    _external_tps_url: MutableMapping[TP, str]
    _topic_groups: MutableMapping[str, int]

    def __init__(self, app: AppT, replicas: int = 0) -> None:
        AbstractPartitionAssignor.__init__(self)
        self.app = app
        self._table_manager = self.app.tables
        self._assignment = ClientAssignment(actives={}, standbys={})
        self._changelog_distribution = {}
        self._external_topic_distribution = {}
        self.replicas = replicas
        self._member_urls = {}
        self._tps_url = {}
        self._external_tps_url = {}
        self._active_tps = set()
        self._standby_tps = set()
        self._topic_groups = {}

    def group_for_topic(self, topic: str) -> int:
        return self._topic_groups[topic]

    @property
    def changelog_distribution(self) -> HostToPartitionMap:
        return self._changelog_distribution

    @changelog_distribution.setter
    def changelog_distribution(self, value: HostToPartitionMap) -> None:
        self._changelog_distribution = value
        self._tps_url = {
            TP(topic, partition): url
            for url, tps in self._changelog_distribution.items()
            for topic, partitions in tps.items()
            for partition in partitions
        }

    @property
    def external_topic_distribution(self) -> HostToPartitionMap:
        return self._external_topic_distribution

    @external_topic_distribution.setter
    def external_topic_distribution(self, value: HostToPartitionMap) -> None:
        self._external_topic_distribution = value
        self._external_tps_url = {
            TP(topic, partition): url
            for url, tps in self._external_topic_distribution.items()
            for topic, partitions in tps.items()
            for partition in partitions
        }

    @property
    def _metadata(self) -> ClientMetadata:
        return ClientMetadata(
            assignment=self._assignment,
            url=str(self._url),
            changelog_distribution=self.changelog_distribution,
            external_topic_distribution=self.external_topic_distribution,
            topic_groups=self._topic_groups,
        )

    @property
    def _url(self) -> URL:
        return self.app.conf.canonical_url

    def on_assignment(self, assignment: ConsumerProtocolMemberMetadata) -> None:
        metadata = cast(
            ClientMetadata, ClientMetadata.loads(self._decompress(assignment.user_data))
        )
        self._assignment = metadata.assignment
        self._topic_groups = dict(metadata.topic_groups)
        self._active_tps = self._assignment.active_tps
        self._standby_tps = self._assignment.standby_tps
        self.changelog_distribution = metadata.changelog_distribution
        self.external_topic_distribution = metadata.external_topic_distribution
        a = sorted(assignment.assignment)
        b = sorted(self._assignment.kafka_protocol_assignment(self._table_manager))
        assert a == b, f"{a!r} != {b!r}"
        assert metadata.url == str(self._url)

    def metadata(self, topics: Set[str]) -> ConsumerProtocolMemberMetadata:
        return ConsumerProtocolMemberMetadata(
            self.version, list(topics), self._metadata.dumps()
        )

    @classmethod
    def _group_co_subscribed(
        cls,
        topics: Set[str],
        subscriptions: MemberSubscriptionMapping,
    ) -> Iterable[Set[str]]:
        topic_subscriptions: MutableMapping[str, Set[str]] = defaultdict(set)
        for client, subscription in subscriptions.items():
            for topic in subscription:
                topic_subscriptions[topic].add(client)
        co_subscribed: MutableMapping[Sequence[str], Set[str]] = defaultdict(set)
        for topic in topics:
            clients = topic_subscriptions[topic]
            assert clients, "Subscribed clients for topic cannot be empty"
            co_subscribed[tuple(clients)].add(topic)
        return co_subscribed.values()

    @classmethod
    def _get_copartitioned_groups(
        cls,
        topics: Set[str],
        cluster: ClusterMetadata,
        subscriptions: MemberSubscriptionMapping,
    ) -> CopartitionedGroups:
        topics_by_partitions: MutableMapping[int, Set] = defaultdict(set)
        for topic in topics:
            num_partitions = len(cluster.partitions_for_topic(topic) or set())
            if num_partitions == 0:
                logger.warning("Ignoring missing topic: %r", topic)
                continue
            topics_by_partitions[num_partitions].add(topic)
        # We group copartitioned topics by subscribed clients such that
        # a group of co-subscribed topics with the same number of partitions
        # are copartitioned
        copart_grouped = {
            num_partitions: cls._group_co_subscribed(topics, subscriptions)
            for num_partitions, topics in topics_by_partitions.items()
        }
        return copart_grouped

    @classmethod
    def _get_client_metadata(
        cls, metadata: ConsumerProtocolMemberMetadata
    ) -> ClientMetadata:
        client_metadata = ClientMetadata.loads(metadata.user_data)
        return cast(ClientMetadata, client_metadata)

    def _update_member_urls(self, clients_metadata: ClientMetadataMapping) -> None:
        self._member_urls = {
            member_id: client_metadata.url
            for member_id, client_metadata in clients_metadata.items()
        }

    def assign(
        self, cluster: ClusterMetadata, member_metadata: MemberMetadataMapping
    ) -> MemberAssignmentMapping:
        if self.app.tracer:
            return self._trace_assign(cluster, member_metadata)
        else:
            return self._assign(cluster, member_metadata)

    def _trace_assign(
        self, cluster: ClusterMetadata, member_metadata: MemberMetadataMapping
    ) -> MemberAssignmentMapping:
        assert self.app.tracer is not None
        span = self.app.tracer.get_tracer("_faust").start_span(
            operation_name="coordinator_assignment",
            tags={"hostname": socket.gethostname()},
        )
        with span:
            assignment = self._assign(cluster, member_metadata)
            self.app._span_add_default_tags(span)
            span.set_tag("assignment", assignment)
        return assignment

    def _assign(
        self, cluster: ClusterMetadata, member_metadata: MemberMetadataMapping
    ) -> MemberAssignmentMapping:
        sensor_state = self.app.sensors.on_assignment_start(self)
        try:
            assignment = self._perform_assignment(cluster, member_metadata)
        except MemoryError:
            raise
        except Exception as exc:
            self.app.sensors.on_assignment_error(self, sensor_state, exc)
            raise
        else:
            self.app.sensors.on_assignment_completed(self, sensor_state)
        return assignment

    def _perform_assignment(
        self, cluster: ClusterMetadata, member_metadata: MemberMetadataMapping
    ) -> MemberAssignmentMapping:
        cluster_assgn = ClusterAssignment()

        clients_metadata = {
            member_id: self._get_client_metadata(metadata)
            for member_id, metadata in member_metadata.items()
        }

        subscriptions = {
            member_id: cast(List[str], metadata.subscription)
            for member_id, metadata in member_metadata.items()
        }

        for member_id in member_metadata:
            cluster_assgn.add_client(
                member_id, subscriptions[member_id], clients_metadata[member_id]
            )
        topics = cluster_assgn.topics()

        copartitioned_groups = self._get_copartitioned_groups(
            topics, cluster, subscriptions
        )

        self._update_member_urls(clients_metadata)

        # Initialize fresh assignment
        assignments: ClientAssignmentMapping = {
            member_id: ClientAssignment(actives={}, standbys={})
            for member_id in member_metadata
        }

        topic_to_group_id = {}
        partitions_by_topic = {}

        for group_id, (num_partitions, topic_groups) in enumerate(
            sorted(copartitioned_groups.items())
        ):
            for topics in topic_groups:
                for topic in topics:
                    topic_to_group_id[topic] = group_id
                    partitions_by_topic[topic] = num_partitions
                assert len(topics) > 0 and num_partitions > 0
                # Get assignment for unique copartitioned group
                assgn = cluster_assgn.copartitioned_assignments(topics)
                assignor = CopartitionedAssignor(
                    topics=topics,
                    cluster_asgn=assgn,
                    num_partitions=num_partitions,
                    replicas=self.replicas,
                )
                # Update client assignments for copartitioned group
                for client, copart_assn in assignor.get_assignment().items():
                    assignments[client].add_copartitioned_assignment(copart_assn)

        # Add all changelogs of global tables as standby for all members
        assignments = self._global_table_standby_assignments(
            assignments, partitions_by_topic
        )

        changelog_distribution = self._get_changelog_distribution(assignments)
        external_topic_distribution = self._get_external_topic_distribution(assignments)
        res = self._protocol_assignments(
            assignments,
            changelog_distribution,
            external_topic_distribution,
            topic_to_group_id,
        )
        return res

    def _global_table_standby_assignments(
        self,
        assignments: ClientAssignmentMapping,
        partitions_by_topic: Mapping[str, int],
    ) -> ClientAssignmentMapping:
        # Ensures all members have access to all changelog partitions
        # as standbys, if not already as actives
        for table in self._table_manager.data.values():
            # Add changelog standbys only if global table
            if table.is_global:
                changelog_topic_name = table._changelog_topic_name()
                num_partitions = partitions_by_topic[changelog_topic_name]
                assert num_partitions is not None
                all_partitions = set(range(0, num_partitions))
                for assignment in assignments.values():
                    active_partitions = set(
                        assignment.actives.get(changelog_topic_name, [])
                    )
                    # Only add those partitions as standby which aren't active
                    standby_partitions = all_partitions - active_partitions
                    assignment.standbys[changelog_topic_name] = list(standby_partitions)
                    # We add all_partitions as active so they are recovered
                    # in the beginning.
                    assignment.actives[changelog_topic_name] = list(all_partitions)
        return assignments

    def _protocol_assignments(
        self,
        assignments: ClientAssignmentMapping,
        cl_distribution: HostToPartitionMap,
        tp_distribution: HostToPartitionMap,
        topic_groups: Mapping[str, int],
    ) -> MemberAssignmentMapping:
        return {
            client: ConsumerProtocolMemberAssignment(
                self.version,
                sorted(assignment.kafka_protocol_assignment(self._table_manager)),
                self._compress(
                    ClientMetadata(
                        assignment=assignment,
                        url=self._member_urls[client],
                        changelog_distribution=cl_distribution,
                        external_topic_distribution=tp_distribution,
                        topic_groups=topic_groups,
                    ).dumps(),
                ),
            )
            for client, assignment in assignments.items()
        }

    @classmethod
    def _compress(cls, raw: bytes) -> bytes:
        return zlib.compress(raw)

    @classmethod
    def _decompress(cls, compressed: bytes) -> bytes:
        return zlib.decompress(compressed)

    @classmethod
    def _topics_filtered(
        cls, assignment: TopicToPartitionMap, topics: Set[str]
    ) -> TopicToPartitionMap:
        return {
            topic: partitions
            for topic, partitions in assignment.items()
            if topic in topics
        }

    @classmethod
    def _non_table_topics_filtered(
        cls, assignment: TopicToPartitionMap, topics: Set[str]
    ) -> TopicToPartitionMap:
        return {
            topic: partitions
            for topic, partitions in assignment.items()
            if topic not in topics
        }

    def _get_changelog_distribution(
        self, assignments: ClientAssignmentMapping
    ) -> HostToPartitionMap:
        topics = self._table_manager.changelog_topics
        return {
            self._member_urls[client]: self._topics_filtered(assignment.actives, topics)
            for client, assignment in assignments.items()
        }

    def _get_external_topic_distribution(
        self, assignments: ClientAssignmentMapping
    ) -> HostToPartitionMap:
        topics = self._table_manager.changelog_topics
        return {
            self._member_urls[client]: self._non_table_topics_filtered(
                assignment.actives, topics
            )
            for client, assignment in assignments.items()
        }

    @property
    def name(self) -> str:
        return "faust"

    @property
    def version(self) -> int:
        return 4

    def assigned_standbys(self) -> Set[TP]:
        return {
            TP(topic, partition)
            for topic, partitions in self._assignment.standbys.items()
            for partition in partitions
        }

    def assigned_actives(self) -> Set[TP]:
        return {
            TP(topic, partition)
            for topic, partitions in self._assignment.actives.items()
            for partition in partitions
        }

    def table_metadata(self, topic: str) -> HostToPartitionMap:
        return {
            host: self._topics_filtered(assignment, {topic})
            for host, assignment in self.changelog_distribution.items()
        }

    def tables_metadata(self) -> HostToPartitionMap:
        return self.changelog_distribution

    def external_topics_metadata(self) -> HostToPartitionMap:
        return self.external_topic_distribution

    def key_store(self, topic: str, key: bytes) -> URL:
        return URL(self._tps_url[self.app.producer.key_partition(topic, key)])

    def external_key_store(self, topic: str, key: bytes) -> URL:
        return URL(self._external_tps_url[self.app.producer.key_partition(topic, key)])

    def is_active(self, tp: TP) -> bool:
        return tp in self._active_tps

    def is_standby(self, tp: TP) -> bool:
        return tp in self._standby_tps
