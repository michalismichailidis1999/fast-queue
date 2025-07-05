#pragma once
#include <chrono>
#include <string>

static const std::string VERSION = "1.0.0";
static const unsigned long VERSION_INT_FORMAT = 0b00000000'00000001'00000000'00000000;

static const std::string FILE_EXTENSION = ".txt";

static const unsigned int MAXIMUM_OPEN_FILE_DESCRIPTORS = 750;

static const std::string CLUSTER_METADATA_QUEUE_NAME = "__cluster_metadata";

static const unsigned int HEARTBEAT_SIGNAL_MIN_BOUND = 1500;
static const unsigned int HEARTBEAT_SIGNAL_MAX_BOUND = 5000;
static const unsigned int LEADER_TIMEOUT = 1000;
static const unsigned int CHECK_FOR_UNAPPLIED_COMMANDS = 1500;
static const unsigned int CHECK_FOR_SETTINGS_UPDATE = 5000;
static const unsigned int CHECK_FOR_COMPACTION = 5000;

static const unsigned int MAX_QUEUE_NAME_CHARS = 100;
static const unsigned int MAX_MESSAGE_KEY_CHARS = 40;

static const unsigned int MAX_QUEUE_PARTITIONS = 1000;

static const unsigned int MAX_ADDRESS_CHARS = 39;

static const unsigned int INDEX_PAGE_SIZE = 4096; // 4KB

static const unsigned int READ_MESSAGES_BATCH_SIZE = 4096 * 4; // 16KB

static const unsigned int MESSAGES_LOC_MAP_PAGE_SIZE = 4096; // 4KB
// first position will hold starting segment id of the next N segments contained in the page 
static const unsigned int MAPPED_SEGMENTS_PER_PAGE = MESSAGES_LOC_MAP_PAGE_SIZE / sizeof(unsigned long long) - 1;

static const unsigned long long MAX_SEGMENT_SIZE = 1073741824 * 2; // 2 GB
static const unsigned long long MAX_COMPACTED_SEGMENT_SIZE = LLONG_MAX; // 8192 PB

static const unsigned int TOTAL_METADATA_BYTES = sizeof(unsigned int);
static const unsigned int TOTAL_METADATA_BYTES_OFFSET = 0;
static const unsigned int VERSION_SIZE = sizeof(unsigned int);
static const unsigned int VERSION_SIZE_OFFSET = TOTAL_METADATA_BYTES + TOTAL_METADATA_BYTES_OFFSET;
static const unsigned int CHECKSUM_SIZE = sizeof(unsigned long long);
static const unsigned int CHECKSUM_OFFSET = VERSION_SIZE + VERSION_SIZE_OFFSET;
static const unsigned int OBJECT_TYPE_SIZE = sizeof(unsigned int);
static const unsigned int OBJECT_TYPE_OFFSET = CHECKSUM_SIZE + CHECKSUM_OFFSET;
static const unsigned int COMMON_METADATA_TOTAL_BYTES = TOTAL_METADATA_BYTES + VERSION_SIZE + CHECKSUM_SIZE + OBJECT_TYPE_SIZE;

static const unsigned int INDEX_PAGE_OFFSET_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_PAGE_OFFSET_OFFSET = COMMON_METADATA_TOTAL_BYTES;
static const unsigned int INDEX_PAGE_TYPE_SIZE = sizeof(unsigned int);
static const unsigned int INDEX_PAGE_TYPE_OFFSET = INDEX_PAGE_OFFSET_SIZE + INDEX_PAGE_OFFSET_OFFSET;
static const unsigned int INDEX_PAGE_MIN_KEY_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_PAGE_MIN_KEY_OFFSET = INDEX_PAGE_TYPE_SIZE + INDEX_PAGE_TYPE_OFFSET;
static const unsigned int INDEX_PAGE_MAX_KEY_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_PAGE_MAX_KEY_OFFSET = INDEX_PAGE_MIN_KEY_SIZE + INDEX_PAGE_MIN_KEY_OFFSET;
static const unsigned int INDEX_PAGE_NUM_OF_ROWS_SIZE = sizeof(unsigned int);
static const unsigned int INDEX_PAGE_NUM_OF_ROWS_OFFSET = INDEX_PAGE_MAX_KEY_SIZE + INDEX_PAGE_MAX_KEY_OFFSET;
static const unsigned int INDEX_PAGE_PARENT_PAGE_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_PAGE_PARENT_PAGE_OFFSET = INDEX_PAGE_NUM_OF_ROWS_SIZE + INDEX_PAGE_NUM_OF_ROWS_OFFSET;
static const unsigned int INDEX_PAGE_PREV_PAGE_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_PAGE_PREV_PAGE_OFFSET = INDEX_PAGE_PARENT_PAGE_SIZE + INDEX_PAGE_PARENT_PAGE_OFFSET;
static const unsigned int INDEX_PAGE_NEXT_PAGE_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_PAGE_NEXT_PAGE_OFFSET = INDEX_PAGE_PREV_PAGE_SIZE + INDEX_PAGE_PREV_PAGE_OFFSET;
static const unsigned int INDEX_PAGE_METADATA_SIZE = COMMON_METADATA_TOTAL_BYTES + INDEX_PAGE_TYPE_SIZE + INDEX_PAGE_MIN_KEY_SIZE 
+ INDEX_PAGE_MAX_KEY_SIZE + INDEX_PAGE_NUM_OF_ROWS_SIZE + INDEX_PAGE_PARENT_PAGE_SIZE + INDEX_PAGE_PREV_PAGE_SIZE + INDEX_PAGE_NEXT_PAGE_SIZE;

static const unsigned int INDEX_KEY_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_KEY_OFFSET = 0;
static const unsigned int INDEX_VALUE_POSITION_SIZE = sizeof(unsigned long long);
static const unsigned int INDEX_VALUE_POSITION_OFFSET = INDEX_KEY_SIZE + INDEX_KEY_OFFSET;
static const unsigned int INDEX_KEY_VALUE_METADATA_SIZE = INDEX_KEY_SIZE + INDEX_VALUE_POSITION_SIZE;

static const unsigned int INDEX_PAGE_TOTAL_ROWS = (INDEX_PAGE_SIZE - INDEX_PAGE_METADATA_SIZE) / INDEX_KEY_VALUE_METADATA_SIZE;

static const unsigned int MARKER_TYPE_SIZE = sizeof(unsigned int);
static const unsigned int MARKER_TYPE_OFFSET = COMMON_METADATA_TOTAL_BYTES;

static const unsigned int MESSAGE_ID_SIZE = sizeof(unsigned long long); // will be message offset or metadata version of cluster
static const unsigned int MESSAGE_ID_OFFSET = COMMON_METADATA_TOTAL_BYTES;
static const unsigned int MESSAGE_TIMESTAMP_SIZE = sizeof(unsigned long long);
static const unsigned int MESSAGE_TIMESTAMP_OFFSET = MESSAGE_ID_SIZE + MESSAGE_ID_OFFSET;
static const unsigned int MESSAGE_IS_ACTIVE_SIZE = sizeof(bool);
static const unsigned int MESSAGE_IS_ACTIVE_OFFSET = MESSAGE_TIMESTAMP_SIZE + MESSAGE_TIMESTAMP_OFFSET;
static const unsigned int MESSAGE_KEY_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int MESSAGE_KEY_LENGTH_OFFSET = MESSAGE_IS_ACTIVE_SIZE + MESSAGE_IS_ACTIVE_OFFSET;
static const unsigned int MESSAGE_KEY_SIZE = sizeof(char) * MAX_MESSAGE_KEY_CHARS;
static const unsigned int MESSAGE_KEY_OFFSET = MESSAGE_KEY_LENGTH_SIZE + MESSAGE_KEY_LENGTH_OFFSET;
static const unsigned int MESSAGE_TOTAL_BYTES = COMMON_METADATA_TOTAL_BYTES + MESSAGE_ID_SIZE + MESSAGE_TIMESTAMP_SIZE + MESSAGE_IS_ACTIVE_SIZE + MESSAGE_KEY_LENGTH_SIZE + MESSAGE_KEY_SIZE;

static const unsigned int QUEUE_NAME_SIZE = MAX_QUEUE_NAME_CHARS * sizeof(char);
static const unsigned int QUEUE_NAME_OFFSET = COMMON_METADATA_TOTAL_BYTES;
static const unsigned int QUEUE_NAME_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int QUEUE_NAME_LENGTH_OFFSET = QUEUE_NAME_SIZE + QUEUE_NAME_OFFSET;
static const unsigned int QUEUE_PARTITIONS_SIZE = sizeof(unsigned int);
static const unsigned int QUEUE_PARTITIONS_OFFSET = QUEUE_NAME_LENGTH_SIZE + QUEUE_NAME_LENGTH_OFFSET;
static const unsigned int QUEUE_REPLICATION_FACTOR_SIZE = sizeof(unsigned int);
static const unsigned int QUEUE_REPLICATION_FACTOR_OFFSET = QUEUE_PARTITIONS_SIZE + QUEUE_PARTITIONS_OFFSET;
static const unsigned int QUEUE_LAST_COMMIT_INDEX_SIZE = sizeof(unsigned long long);
static const unsigned int QUEUE_LAST_COMMIT_INDEX_OFFSET = QUEUE_REPLICATION_FACTOR_SIZE + QUEUE_REPLICATION_FACTOR_OFFSET;
static const unsigned int QUEUE_LAST_APPLIED_INDEX_SIZE = sizeof(unsigned long long);
static const unsigned int QUEUE_LAST_APPLIED_INDEX_OFFSET = QUEUE_LAST_COMMIT_INDEX_SIZE + QUEUE_LAST_COMMIT_INDEX_OFFSET;
static const unsigned int QUEUE_CLEANUP_POLICY_SIZE = sizeof(unsigned int);
static const unsigned int QUEUE_CLEANUP_POLICY_OFFSET = QUEUE_LAST_APPLIED_INDEX_SIZE + QUEUE_LAST_APPLIED_INDEX_OFFSET;
static const unsigned int QUEUE_METADATA_TOTAL_BYTES = COMMON_METADATA_TOTAL_BYTES + QUEUE_NAME_SIZE + QUEUE_NAME_LENGTH_SIZE 
+ QUEUE_PARTITIONS_SIZE + QUEUE_REPLICATION_FACTOR_SIZE + QUEUE_LAST_COMMIT_INDEX_SIZE + QUEUE_LAST_APPLIED_INDEX_SIZE + QUEUE_CLEANUP_POLICY_SIZE;

static const unsigned int SEGMENT_ID_SIZE = sizeof(unsigned long long);
static const unsigned int SEGMENT_ID_OFFSET = COMMON_METADATA_TOTAL_BYTES;
static const unsigned int SEGMENT_LAST_MESSAGE_TMSTMP_SIZE = sizeof(unsigned long long);
static const unsigned int SEGMENT_LAST_MESSAGE_TMSTMP_OFFSET = SEGMENT_ID_SIZE + SEGMENT_ID_OFFSET;
static const unsigned int SEGMENT_LAST_MESSAGE_OFF_SIZE = sizeof(unsigned long long);
static const unsigned int SEGMENT_LAST_MESSAGE_OFF_OFFSET = SEGMENT_LAST_MESSAGE_TMSTMP_SIZE + SEGMENT_LAST_MESSAGE_TMSTMP_OFFSET;
static const unsigned int SEGMENT_IS_READ_ONLY_SIZE = sizeof(bool);
static const unsigned int SEGMENT_IS_READ_ONLY_OFFSET = SEGMENT_LAST_MESSAGE_OFF_SIZE + SEGMENT_LAST_MESSAGE_OFF_OFFSET;
static const unsigned int SEGMENT_IS_COMPACTED_SIZE = sizeof(bool);
static const unsigned int SEGMENT_IS_COMPACTED_OFFSET = SEGMENT_IS_READ_ONLY_SIZE + SEGMENT_IS_READ_ONLY_OFFSET;
static const unsigned int SEGMENT_LAST_INDEX_PAGE_OFFSET_SIZE = sizeof(unsigned int);
static const unsigned int SEGMENT_LAST_INDEX_PAGE_OFFSET_OFFSET = SEGMENT_IS_COMPACTED_SIZE + SEGMENT_IS_COMPACTED_OFFSET;
static const unsigned int SEGMENT_METADATA_TOTAL_BYTES = COMMON_METADATA_TOTAL_BYTES + SEGMENT_ID_SIZE + SEGMENT_LAST_MESSAGE_TMSTMP_SIZE
+ SEGMENT_LAST_MESSAGE_OFF_SIZE + SEGMENT_IS_READ_ONLY_SIZE + SEGMENT_IS_COMPACTED_SIZE + SEGMENT_LAST_INDEX_PAGE_OFFSET_SIZE;

// Commands

static const unsigned int COMMAND_TYPE_SIZE = sizeof(unsigned int);
static const unsigned int COMMAND_TYPE_OFFSET = MESSAGE_TOTAL_BYTES;
static const unsigned int COMMAND_TERM_SIZE = sizeof(unsigned long long);
static const unsigned int COMMAND_TERM_OFFSET = COMMAND_TYPE_SIZE + COMMAND_TYPE_OFFSET;
static const unsigned int COMMAND_TOTAL_BYTES = MESSAGE_TOTAL_BYTES + COMMAND_TYPE_SIZE + COMMAND_TERM_SIZE;

static const unsigned int CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET = COMMAND_TOTAL_BYTES;
static const unsigned int CQ_COMMAND_QUEUE_NAME_SIZE = sizeof(char) * MAX_QUEUE_NAME_CHARS;
static const unsigned int CQ_COMMAND_QUEUE_NAME_OFFSET = CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE + CQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET;
static const unsigned int CQ_COMMAND_PARTITION_SIZE = sizeof(unsigned int);
static const unsigned int CQ_COMMAND_PARTITION_OFFSET = CQ_COMMAND_QUEUE_NAME_SIZE + CQ_COMMAND_QUEUE_NAME_OFFSET;
static const unsigned int CQ_COMMAND_REPLICATION_SIZE = sizeof(unsigned int);
static const unsigned int CQ_COMMAND_REPLICATION_OFFSET = CQ_COMMAND_PARTITION_SIZE + CQ_COMMAND_PARTITION_OFFSET;
static const unsigned int CQ_COMMAND_TOTAL_BYTES = COMMAND_TOTAL_BYTES + CQ_COMMAND_QUEUE_NAME_LENGTH_SIZE + CQ_COMMAND_QUEUE_NAME_SIZE
+ CQ_COMMAND_PARTITION_SIZE + CQ_COMMAND_REPLICATION_SIZE;

static const unsigned int PA_COMMAND_QUEUE_NAME_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET = COMMAND_TOTAL_BYTES;
static const unsigned int PA_COMMAND_QUEUE_NAME_SIZE = sizeof(char) * MAX_QUEUE_NAME_CHARS;
static const unsigned int PA_COMMAND_QUEUE_NAME_OFFSET = PA_COMMAND_QUEUE_NAME_LENGTH_SIZE + PA_COMMAND_QUEUE_NAME_LENGTH_OFFSET;
static const unsigned int PA_COMMAND_PARTITION_SIZE = sizeof(unsigned int);
static const unsigned int PA_COMMAND_PARTITION_OFFSET = PA_COMMAND_QUEUE_NAME_SIZE + PA_COMMAND_QUEUE_NAME_OFFSET;
static const unsigned int PA_COMMAND_TO_NODE_SIZE = sizeof(unsigned int);
static const unsigned int PA_COMMAND_TO_NODE_OFFSET = PA_COMMAND_PARTITION_SIZE + PA_COMMAND_PARTITION_OFFSET;
static const unsigned int PA_COMMAND_FROM_NODE_SIZE = sizeof(unsigned int);
static const unsigned int PA_COMMAND_FROM_NODE_OFFSET = PA_COMMAND_TO_NODE_SIZE + PA_COMMAND_TO_NODE_OFFSET;
static const unsigned int PA_COMMAND_TOTAL_BYTES = COMMAND_TOTAL_BYTES + PA_COMMAND_QUEUE_NAME_LENGTH_SIZE + PA_COMMAND_QUEUE_NAME_SIZE
+ PA_COMMAND_PARTITION_SIZE + PA_COMMAND_TO_NODE_SIZE + PA_COMMAND_FROM_NODE_SIZE;

static const unsigned int PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET = COMMAND_TOTAL_BYTES;
static const unsigned int PLA_COMMAND_QUEUE_NAME_SIZE = sizeof(char) * MAX_QUEUE_NAME_CHARS;
static const unsigned int PLA_COMMAND_QUEUE_NAME_OFFSET = PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE + PLA_COMMAND_QUEUE_NAME_LENGTH_OFFSET;
static const unsigned int PLA_COMMAND_PARTITION_SIZE = sizeof(unsigned int);
static const unsigned int PLA_COMMAND_PARTITION_OFFSET = PLA_COMMAND_QUEUE_NAME_SIZE + PLA_COMMAND_QUEUE_NAME_OFFSET;
static const unsigned int PLA_COMMAND_NEW_LEADER_SIZE = sizeof(unsigned int);
static const unsigned int PLA_COMMAND_NEW_LEADER_OFFSET = PLA_COMMAND_PARTITION_SIZE + PLA_COMMAND_PARTITION_OFFSET;
static const unsigned int PLA_COMMAND_PREV_LEADER_SIZE = sizeof(unsigned int);
static const unsigned int PLA_COMMAND_PREV_LEADER_OFFSET = PLA_COMMAND_NEW_LEADER_SIZE + PLA_COMMAND_NEW_LEADER_OFFSET;
static const unsigned int PLA_COMMAND_TOTAL_BYTES = COMMAND_TOTAL_BYTES + PLA_COMMAND_QUEUE_NAME_LENGTH_SIZE + PLA_COMMAND_QUEUE_NAME_SIZE
+ PLA_COMMAND_PARTITION_SIZE + PLA_COMMAND_NEW_LEADER_SIZE + PLA_COMMAND_PREV_LEADER_SIZE;

static const unsigned int DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET = COMMAND_TOTAL_BYTES;
static const unsigned int DQ_COMMAND_QUEUE_NAME_SIZE = sizeof(char) * MAX_QUEUE_NAME_CHARS;
static const unsigned int DQ_COMMAND_QUEUE_NAME_OFFSET = DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE + DQ_COMMAND_QUEUE_NAME_LENGTH_OFFSET;
static const unsigned int DQ_COMMAND_TOTAL_BYTES = COMMAND_TOTAL_BYTES + DQ_COMMAND_QUEUE_NAME_LENGTH_SIZE + DQ_COMMAND_QUEUE_NAME_SIZE;

static const unsigned int RDN_COMMAND_NODE_ID_SIZE = sizeof(unsigned int);
static const unsigned int RDN_COMMAND_NODE_ID_OFFSET = COMMAND_TOTAL_BYTES;
static const unsigned int RDN_COMMAND_ADDRESS_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int RDN_COMMAND_ADDRESS_LENGTH_OFFSET = RDN_COMMAND_NODE_ID_SIZE + RDN_COMMAND_NODE_ID_OFFSET;
static const unsigned int RDN_COMMAND_ADDRESS_SIZE = sizeof(char) * MAX_ADDRESS_CHARS;
static const unsigned int RDN_COMMAND_ADDRESS_OFFSET = RDN_COMMAND_ADDRESS_LENGTH_SIZE + RDN_COMMAND_ADDRESS_LENGTH_OFFSET;
static const unsigned int RDN_COMMAND_PORT_SIZE = sizeof(unsigned int);
static const unsigned int RDN_COMMAND_PORT_OFFSET = RDN_COMMAND_ADDRESS_SIZE + RDN_COMMAND_ADDRESS_OFFSET;
static const unsigned int RDN_COMMAND_TOTAL_BYTES = COMMAND_TOTAL_BYTES + RDN_COMMAND_NODE_ID_SIZE + RDN_COMMAND_ADDRESS_LENGTH_SIZE + RDN_COMMAND_ADDRESS_SIZE + RDN_COMMAND_PORT_SIZE;

static const unsigned int UDN_COMMAND_NODE_ID_SIZE = sizeof(unsigned int);
static const unsigned int UDN_COMMAND_NODE_ID_OFFSET = COMMAND_TOTAL_BYTES;
static const unsigned int UDN_COMMAND_TOTAL_BYTES = COMMAND_TOTAL_BYTES + UDN_COMMAND_NODE_ID_SIZE;

// =================================================================

// Cluster Metadata

static const unsigned int METADATA_VERSION_SIZE = sizeof(unsigned long long);
static const unsigned int METADATA_VERSION_OFFSET = COMMON_METADATA_TOTAL_BYTES;
static const unsigned int TERM_SIZE = sizeof(unsigned long long);
static const unsigned int TERM_OFFSET = METADATA_VERSION_SIZE + METADATA_VERSION_OFFSET;
static const unsigned int TOTAL_QUEUES_SIZE = sizeof(unsigned int);
static const unsigned int TOTAL_QUEUES_OFFSET = TERM_SIZE + TERM_OFFSET;
static const unsigned int TOTAL_PARTITION_ASSIGNMENTS_SIZE = sizeof(unsigned int);
static const unsigned int TOTAL_PARTITION_ASSIGNMENTS_OFFSET = TOTAL_QUEUES_SIZE + TOTAL_QUEUES_OFFSET;
static const unsigned int TOTAL_PARTITION_ASSIGNMENTS_BYTES = COMMON_METADATA_TOTAL_BYTES + METADATA_VERSION_SIZE + TERM_SIZE + TOTAL_QUEUES_SIZE + TOTAL_PARTITION_ASSIGNMENTS_SIZE;

static const unsigned int PA_QUEUE_NAME_LENGTH_SIZE = sizeof(unsigned int);
static const unsigned int PA_QUEUE_NAME_LENGTH_OFFSET = 0;
static const unsigned int PA_QUEUE_NAME_SIZE = sizeof(char) * MAX_QUEUE_NAME_CHARS;
static const unsigned int PA_QUEUE_NAME_OFFSET = PA_QUEUE_NAME_LENGTH_SIZE + PA_QUEUE_NAME_LENGTH_OFFSET;
static const unsigned int PA_QUEUE_TOTAL_ASSIGNMENTS_SIZE = sizeof(char) * MAX_QUEUE_NAME_CHARS;
static const unsigned int PA_QUEUE_TOTAL_ASSIGNMENTS_OFFSET = PA_QUEUE_NAME_SIZE + PA_QUEUE_NAME_OFFSET;
static const unsigned int PA_QUEUE_TOTAL_BYTES = PA_QUEUE_NAME_LENGTH_SIZE + PA_QUEUE_NAME_SIZE + PA_QUEUE_TOTAL_ASSIGNMENTS_SIZE;

static const unsigned int PA_PARTITION_ID_SIZE = sizeof(unsigned int);
static const unsigned int PA_PARTITION_ID_OFFSET = 0;
static const unsigned int PA_NODE_ID_SIZE = sizeof(unsigned int);
static const unsigned int PA_NODE_ID_OFFSET = PA_PARTITION_ID_SIZE + PA_PARTITION_ID_OFFSET;
static const unsigned int PA_IS_LEAD_SIZE = sizeof(bool);
static const unsigned int PA_IS_LEAD_OFFSET = PA_NODE_ID_SIZE + PA_NODE_ID_OFFSET;
static const unsigned int PA_NODE_ASSIGNMENT_TOTAL_BYTES = PA_PARTITION_ID_SIZE + PA_NODE_ID_SIZE + PA_IS_LEAD_SIZE;

// =================================================================