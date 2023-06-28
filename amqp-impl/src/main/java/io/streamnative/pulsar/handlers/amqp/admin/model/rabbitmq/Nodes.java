/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import java.util.List;

public class Nodes {

    private String os_pid;
    private int fd_total;
    private int sockets_total;
    private long mem_limit;
    private boolean mem_alarm;
    private long disk_free_limit;
    private boolean disk_free_alarm;
    private int proc_total;
    private String rates_mode;
    private long uptime;
    private int run_queue;
    private int processors;
    private String db_dir;
    private int net_ticktime;
    private String mem_calculation_strategy;
    private String name;
    private String type;
    private boolean running;
    private long mem_used;
    private MemUsedDetailsBean mem_used_details;
    private int fd_used;
    private FdUsedDetailsBean fd_used_details;
    private int sockets_used;
    private SocketsUsedDetailsBean sockets_used_details;
    private int proc_used;
    private ProcUsedDetailsBean proc_used_details;
    private long disk_free;
    private DiskFreeDetailsBean disk_free_details;
    private long gc_num;
    private GcNumDetailsBean gc_num_details;
    private long gc_bytes_reclaimed;
    private GcBytesReclaimedDetailsBean gc_bytes_reclaimed_details;
    private long context_switches;
    private ContextSwitchesDetailsBean context_switches_details;
    private int io_read_count;
    private IoReadCountDetailsBean io_read_count_details;
    private long io_read_bytes;
    private IoReadBytesDetailsBean io_read_bytes_details;
    private double io_read_avg_time;
    private IoReadAvgTimeDetailsBean io_read_avg_time_details;
    private int io_write_count;
    private IoWriteCountDetailsBean io_write_count_details;
    private long io_write_bytes;
    private IoWriteBytesDetailsBean io_write_bytes_details;
    private double io_write_avg_time;
    private IoWriteAvgTimeDetailsBean io_write_avg_time_details;
    private int io_sync_count;
    private IoSyncCountDetailsBean io_sync_count_details;
    private double io_sync_avg_time;
    private IoSyncAvgTimeDetailsBean io_sync_avg_time_details;
    private int io_seek_count;
    private IoSeekCountDetailsBean io_seek_count_details;
    private double io_seek_avg_time;
    private IoSeekAvgTimeDetailsBean io_seek_avg_time_details;
    private int io_reopen_count;
    private IoReopenCountDetailsBean io_reopen_count_details;
    private int mnesia_ram_tx_count;
    private MnesiaRamTxCountDetailsBean mnesia_ram_tx_count_details;
    private int mnesia_disk_tx_count;
    private MnesiaDiskTxCountDetailsBean mnesia_disk_tx_count_details;
    private int msg_store_read_count;
    private MsgStoreReadCountDetailsBean msg_store_read_count_details;
    private int msg_store_write_count;
    private MsgStoreWriteCountDetailsBean msg_store_write_count_details;
    private long queue_index_journal_write_count;
    private QueueIndexJournalWriteCountDetailsBean queue_index_journal_write_count_details;
    private int queue_index_write_count;
    private QueueIndexWriteCountDetailsBean queue_index_write_count_details;
    private int queue_index_read_count;
    private QueueIndexReadCountDetailsBean queue_index_read_count_details;
    private long io_file_handle_open_attempt_count;
    private IoFileHandleOpenAttemptCountDetailsBean io_file_handle_open_attempt_count_details;
    private double io_file_handle_open_attempt_avg_time;
    private IoFileHandleOpenAttemptAvgTimeDetailsBean io_file_handle_open_attempt_avg_time_details;
    private int connection_created;
    private ConnectionCreatedDetailsBean connection_created_details;
    private int connection_closed;
    private ConnectionClosedDetailsBean connection_closed_details;
    private int channel_created;
    private ChannelCreatedDetailsBean channel_created_details;
    private int channel_closed;
    private ChannelClosedDetailsBean channel_closed_details;
    private int queue_declared;
    private QueueDeclaredDetailsBean queue_declared_details;
    private int queue_created;
    private QueueCreatedDetailsBean queue_created_details;
    private int queue_deleted;
    private QueueDeletedDetailsBean queue_deleted_details;
    private MetricsGcQueueLengthBean metrics_gc_queue_length;
    private List<?> partitions;
    private List<ExchangeTypesBean> exchange_types;
    private List<AuthMechanismsBean> auth_mechanisms;
    private List<ApplicationsBean> applications;
    private List<ContextsBean> contexts;
    private List<String> log_files;
    private List<String> config_files;
    private List<String> enabled_plugins;
    private List<?> cluster_links;

    public String getOs_pid() {
        return os_pid;
    }

    public void setOs_pid(String os_pid) {
        this.os_pid = os_pid;
    }

    public int getFd_total() {
        return fd_total;
    }

    public void setFd_total(int fd_total) {
        this.fd_total = fd_total;
    }

    public int getSockets_total() {
        return sockets_total;
    }

    public void setSockets_total(int sockets_total) {
        this.sockets_total = sockets_total;
    }

    public long getMem_limit() {
        return mem_limit;
    }

    public void setMem_limit(long mem_limit) {
        this.mem_limit = mem_limit;
    }

    public boolean isMem_alarm() {
        return mem_alarm;
    }

    public void setMem_alarm(boolean mem_alarm) {
        this.mem_alarm = mem_alarm;
    }

    public long getDisk_free_limit() {
        return disk_free_limit;
    }

    public void setDisk_free_limit(long disk_free_limit) {
        this.disk_free_limit = disk_free_limit;
    }

    public boolean isDisk_free_alarm() {
        return disk_free_alarm;
    }

    public void setDisk_free_alarm(boolean disk_free_alarm) {
        this.disk_free_alarm = disk_free_alarm;
    }

    public int getProc_total() {
        return proc_total;
    }

    public void setProc_total(int proc_total) {
        this.proc_total = proc_total;
    }

    public String getRates_mode() {
        return rates_mode;
    }

    public void setRates_mode(String rates_mode) {
        this.rates_mode = rates_mode;
    }

    public long getUptime() {
        return uptime;
    }

    public void setUptime(long uptime) {
        this.uptime = uptime;
    }

    public int getRun_queue() {
        return run_queue;
    }

    public void setRun_queue(int run_queue) {
        this.run_queue = run_queue;
    }

    public int getProcessors() {
        return processors;
    }

    public void setProcessors(int processors) {
        this.processors = processors;
    }

    public String getDb_dir() {
        return db_dir;
    }

    public void setDb_dir(String db_dir) {
        this.db_dir = db_dir;
    }

    public int getNet_ticktime() {
        return net_ticktime;
    }

    public void setNet_ticktime(int net_ticktime) {
        this.net_ticktime = net_ticktime;
    }

    public String getMem_calculation_strategy() {
        return mem_calculation_strategy;
    }

    public void setMem_calculation_strategy(String mem_calculation_strategy) {
        this.mem_calculation_strategy = mem_calculation_strategy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public long getMem_used() {
        return mem_used;
    }

    public void setMem_used(long mem_used) {
        this.mem_used = mem_used;
    }

    public MemUsedDetailsBean getMem_used_details() {
        return mem_used_details;
    }

    public void setMem_used_details(MemUsedDetailsBean mem_used_details) {
        this.mem_used_details = mem_used_details;
    }

    public int getFd_used() {
        return fd_used;
    }

    public void setFd_used(int fd_used) {
        this.fd_used = fd_used;
    }

    public FdUsedDetailsBean getFd_used_details() {
        return fd_used_details;
    }

    public void setFd_used_details(FdUsedDetailsBean fd_used_details) {
        this.fd_used_details = fd_used_details;
    }

    public int getSockets_used() {
        return sockets_used;
    }

    public void setSockets_used(int sockets_used) {
        this.sockets_used = sockets_used;
    }

    public SocketsUsedDetailsBean getSockets_used_details() {
        return sockets_used_details;
    }

    public void setSockets_used_details(SocketsUsedDetailsBean sockets_used_details) {
        this.sockets_used_details = sockets_used_details;
    }

    public int getProc_used() {
        return proc_used;
    }

    public void setProc_used(int proc_used) {
        this.proc_used = proc_used;
    }

    public ProcUsedDetailsBean getProc_used_details() {
        return proc_used_details;
    }

    public void setProc_used_details(ProcUsedDetailsBean proc_used_details) {
        this.proc_used_details = proc_used_details;
    }

    public long getDisk_free() {
        return disk_free;
    }

    public void setDisk_free(long disk_free) {
        this.disk_free = disk_free;
    }

    public DiskFreeDetailsBean getDisk_free_details() {
        return disk_free_details;
    }

    public void setDisk_free_details(DiskFreeDetailsBean disk_free_details) {
        this.disk_free_details = disk_free_details;
    }

    public long getGc_num() {
        return gc_num;
    }

    public void setGc_num(long gc_num) {
        this.gc_num = gc_num;
    }

    public GcNumDetailsBean getGc_num_details() {
        return gc_num_details;
    }

    public void setGc_num_details(GcNumDetailsBean gc_num_details) {
        this.gc_num_details = gc_num_details;
    }

    public long getGc_bytes_reclaimed() {
        return gc_bytes_reclaimed;
    }

    public void setGc_bytes_reclaimed(long gc_bytes_reclaimed) {
        this.gc_bytes_reclaimed = gc_bytes_reclaimed;
    }

    public GcBytesReclaimedDetailsBean getGc_bytes_reclaimed_details() {
        return gc_bytes_reclaimed_details;
    }

    public void setGc_bytes_reclaimed_details(GcBytesReclaimedDetailsBean gc_bytes_reclaimed_details) {
        this.gc_bytes_reclaimed_details = gc_bytes_reclaimed_details;
    }

    public long getContext_switches() {
        return context_switches;
    }

    public void setContext_switches(long context_switches) {
        this.context_switches = context_switches;
    }

    public ContextSwitchesDetailsBean getContext_switches_details() {
        return context_switches_details;
    }

    public void setContext_switches_details(ContextSwitchesDetailsBean context_switches_details) {
        this.context_switches_details = context_switches_details;
    }

    public int getIo_read_count() {
        return io_read_count;
    }

    public void setIo_read_count(int io_read_count) {
        this.io_read_count = io_read_count;
    }

    public IoReadCountDetailsBean getIo_read_count_details() {
        return io_read_count_details;
    }

    public void setIo_read_count_details(IoReadCountDetailsBean io_read_count_details) {
        this.io_read_count_details = io_read_count_details;
    }

    public long getIo_read_bytes() {
        return io_read_bytes;
    }

    public void setIo_read_bytes(long io_read_bytes) {
        this.io_read_bytes = io_read_bytes;
    }

    public IoReadBytesDetailsBean getIo_read_bytes_details() {
        return io_read_bytes_details;
    }

    public void setIo_read_bytes_details(IoReadBytesDetailsBean io_read_bytes_details) {
        this.io_read_bytes_details = io_read_bytes_details;
    }

    public double getIo_read_avg_time() {
        return io_read_avg_time;
    }

    public void setIo_read_avg_time(double io_read_avg_time) {
        this.io_read_avg_time = io_read_avg_time;
    }

    public IoReadAvgTimeDetailsBean getIo_read_avg_time_details() {
        return io_read_avg_time_details;
    }

    public void setIo_read_avg_time_details(IoReadAvgTimeDetailsBean io_read_avg_time_details) {
        this.io_read_avg_time_details = io_read_avg_time_details;
    }

    public int getIo_write_count() {
        return io_write_count;
    }

    public void setIo_write_count(int io_write_count) {
        this.io_write_count = io_write_count;
    }

    public IoWriteCountDetailsBean getIo_write_count_details() {
        return io_write_count_details;
    }

    public void setIo_write_count_details(IoWriteCountDetailsBean io_write_count_details) {
        this.io_write_count_details = io_write_count_details;
    }

    public long getIo_write_bytes() {
        return io_write_bytes;
    }

    public void setIo_write_bytes(long io_write_bytes) {
        this.io_write_bytes = io_write_bytes;
    }

    public IoWriteBytesDetailsBean getIo_write_bytes_details() {
        return io_write_bytes_details;
    }

    public void setIo_write_bytes_details(IoWriteBytesDetailsBean io_write_bytes_details) {
        this.io_write_bytes_details = io_write_bytes_details;
    }

    public double getIo_write_avg_time() {
        return io_write_avg_time;
    }

    public void setIo_write_avg_time(double io_write_avg_time) {
        this.io_write_avg_time = io_write_avg_time;
    }

    public IoWriteAvgTimeDetailsBean getIo_write_avg_time_details() {
        return io_write_avg_time_details;
    }

    public void setIo_write_avg_time_details(IoWriteAvgTimeDetailsBean io_write_avg_time_details) {
        this.io_write_avg_time_details = io_write_avg_time_details;
    }

    public int getIo_sync_count() {
        return io_sync_count;
    }

    public void setIo_sync_count(int io_sync_count) {
        this.io_sync_count = io_sync_count;
    }

    public IoSyncCountDetailsBean getIo_sync_count_details() {
        return io_sync_count_details;
    }

    public void setIo_sync_count_details(IoSyncCountDetailsBean io_sync_count_details) {
        this.io_sync_count_details = io_sync_count_details;
    }

    public double getIo_sync_avg_time() {
        return io_sync_avg_time;
    }

    public void setIo_sync_avg_time(double io_sync_avg_time) {
        this.io_sync_avg_time = io_sync_avg_time;
    }

    public IoSyncAvgTimeDetailsBean getIo_sync_avg_time_details() {
        return io_sync_avg_time_details;
    }

    public void setIo_sync_avg_time_details(IoSyncAvgTimeDetailsBean io_sync_avg_time_details) {
        this.io_sync_avg_time_details = io_sync_avg_time_details;
    }

    public int getIo_seek_count() {
        return io_seek_count;
    }

    public void setIo_seek_count(int io_seek_count) {
        this.io_seek_count = io_seek_count;
    }

    public IoSeekCountDetailsBean getIo_seek_count_details() {
        return io_seek_count_details;
    }

    public void setIo_seek_count_details(IoSeekCountDetailsBean io_seek_count_details) {
        this.io_seek_count_details = io_seek_count_details;
    }

    public double getIo_seek_avg_time() {
        return io_seek_avg_time;
    }

    public void setIo_seek_avg_time(double io_seek_avg_time) {
        this.io_seek_avg_time = io_seek_avg_time;
    }

    public IoSeekAvgTimeDetailsBean getIo_seek_avg_time_details() {
        return io_seek_avg_time_details;
    }

    public void setIo_seek_avg_time_details(IoSeekAvgTimeDetailsBean io_seek_avg_time_details) {
        this.io_seek_avg_time_details = io_seek_avg_time_details;
    }

    public int getIo_reopen_count() {
        return io_reopen_count;
    }

    public void setIo_reopen_count(int io_reopen_count) {
        this.io_reopen_count = io_reopen_count;
    }

    public IoReopenCountDetailsBean getIo_reopen_count_details() {
        return io_reopen_count_details;
    }

    public void setIo_reopen_count_details(IoReopenCountDetailsBean io_reopen_count_details) {
        this.io_reopen_count_details = io_reopen_count_details;
    }

    public int getMnesia_ram_tx_count() {
        return mnesia_ram_tx_count;
    }

    public void setMnesia_ram_tx_count(int mnesia_ram_tx_count) {
        this.mnesia_ram_tx_count = mnesia_ram_tx_count;
    }

    public MnesiaRamTxCountDetailsBean getMnesia_ram_tx_count_details() {
        return mnesia_ram_tx_count_details;
    }

    public void setMnesia_ram_tx_count_details(MnesiaRamTxCountDetailsBean mnesia_ram_tx_count_details) {
        this.mnesia_ram_tx_count_details = mnesia_ram_tx_count_details;
    }

    public int getMnesia_disk_tx_count() {
        return mnesia_disk_tx_count;
    }

    public void setMnesia_disk_tx_count(int mnesia_disk_tx_count) {
        this.mnesia_disk_tx_count = mnesia_disk_tx_count;
    }

    public MnesiaDiskTxCountDetailsBean getMnesia_disk_tx_count_details() {
        return mnesia_disk_tx_count_details;
    }

    public void setMnesia_disk_tx_count_details(MnesiaDiskTxCountDetailsBean mnesia_disk_tx_count_details) {
        this.mnesia_disk_tx_count_details = mnesia_disk_tx_count_details;
    }

    public int getMsg_store_read_count() {
        return msg_store_read_count;
    }

    public void setMsg_store_read_count(int msg_store_read_count) {
        this.msg_store_read_count = msg_store_read_count;
    }

    public MsgStoreReadCountDetailsBean getMsg_store_read_count_details() {
        return msg_store_read_count_details;
    }

    public void setMsg_store_read_count_details(MsgStoreReadCountDetailsBean msg_store_read_count_details) {
        this.msg_store_read_count_details = msg_store_read_count_details;
    }

    public int getMsg_store_write_count() {
        return msg_store_write_count;
    }

    public void setMsg_store_write_count(int msg_store_write_count) {
        this.msg_store_write_count = msg_store_write_count;
    }

    public MsgStoreWriteCountDetailsBean getMsg_store_write_count_details() {
        return msg_store_write_count_details;
    }

    public void setMsg_store_write_count_details(MsgStoreWriteCountDetailsBean msg_store_write_count_details) {
        this.msg_store_write_count_details = msg_store_write_count_details;
    }

    public long getQueue_index_journal_write_count() {
        return queue_index_journal_write_count;
    }

    public void setQueue_index_journal_write_count(long queue_index_journal_write_count) {
        this.queue_index_journal_write_count = queue_index_journal_write_count;
    }

    public QueueIndexJournalWriteCountDetailsBean getQueue_index_journal_write_count_details() {
        return queue_index_journal_write_count_details;
    }

    public void setQueue_index_journal_write_count_details(
            QueueIndexJournalWriteCountDetailsBean queue_index_journal_write_count_details) {
        this.queue_index_journal_write_count_details = queue_index_journal_write_count_details;
    }

    public int getQueue_index_write_count() {
        return queue_index_write_count;
    }

    public void setQueue_index_write_count(int queue_index_write_count) {
        this.queue_index_write_count = queue_index_write_count;
    }

    public QueueIndexWriteCountDetailsBean getQueue_index_write_count_details() {
        return queue_index_write_count_details;
    }

    public void setQueue_index_write_count_details(QueueIndexWriteCountDetailsBean queue_index_write_count_details) {
        this.queue_index_write_count_details = queue_index_write_count_details;
    }

    public int getQueue_index_read_count() {
        return queue_index_read_count;
    }

    public void setQueue_index_read_count(int queue_index_read_count) {
        this.queue_index_read_count = queue_index_read_count;
    }

    public QueueIndexReadCountDetailsBean getQueue_index_read_count_details() {
        return queue_index_read_count_details;
    }

    public void setQueue_index_read_count_details(QueueIndexReadCountDetailsBean queue_index_read_count_details) {
        this.queue_index_read_count_details = queue_index_read_count_details;
    }

    public long getIo_file_handle_open_attempt_count() {
        return io_file_handle_open_attempt_count;
    }

    public void setIo_file_handle_open_attempt_count(long io_file_handle_open_attempt_count) {
        this.io_file_handle_open_attempt_count = io_file_handle_open_attempt_count;
    }

    public IoFileHandleOpenAttemptCountDetailsBean getIo_file_handle_open_attempt_count_details() {
        return io_file_handle_open_attempt_count_details;
    }

    public void setIo_file_handle_open_attempt_count_details(
            IoFileHandleOpenAttemptCountDetailsBean io_file_handle_open_attempt_count_details) {
        this.io_file_handle_open_attempt_count_details = io_file_handle_open_attempt_count_details;
    }

    public double getIo_file_handle_open_attempt_avg_time() {
        return io_file_handle_open_attempt_avg_time;
    }

    public void setIo_file_handle_open_attempt_avg_time(double io_file_handle_open_attempt_avg_time) {
        this.io_file_handle_open_attempt_avg_time = io_file_handle_open_attempt_avg_time;
    }

    public IoFileHandleOpenAttemptAvgTimeDetailsBean getIo_file_handle_open_attempt_avg_time_details() {
        return io_file_handle_open_attempt_avg_time_details;
    }

    public void setIo_file_handle_open_attempt_avg_time_details(
            IoFileHandleOpenAttemptAvgTimeDetailsBean io_file_handle_open_attempt_avg_time_details) {
        this.io_file_handle_open_attempt_avg_time_details = io_file_handle_open_attempt_avg_time_details;
    }

    public int getConnection_created() {
        return connection_created;
    }

    public void setConnection_created(int connection_created) {
        this.connection_created = connection_created;
    }

    public ConnectionCreatedDetailsBean getConnection_created_details() {
        return connection_created_details;
    }

    public void setConnection_created_details(ConnectionCreatedDetailsBean connection_created_details) {
        this.connection_created_details = connection_created_details;
    }

    public int getConnection_closed() {
        return connection_closed;
    }

    public void setConnection_closed(int connection_closed) {
        this.connection_closed = connection_closed;
    }

    public ConnectionClosedDetailsBean getConnection_closed_details() {
        return connection_closed_details;
    }

    public void setConnection_closed_details(ConnectionClosedDetailsBean connection_closed_details) {
        this.connection_closed_details = connection_closed_details;
    }

    public int getChannel_created() {
        return channel_created;
    }

    public void setChannel_created(int channel_created) {
        this.channel_created = channel_created;
    }

    public ChannelCreatedDetailsBean getChannel_created_details() {
        return channel_created_details;
    }

    public void setChannel_created_details(ChannelCreatedDetailsBean channel_created_details) {
        this.channel_created_details = channel_created_details;
    }

    public int getChannel_closed() {
        return channel_closed;
    }

    public void setChannel_closed(int channel_closed) {
        this.channel_closed = channel_closed;
    }

    public ChannelClosedDetailsBean getChannel_closed_details() {
        return channel_closed_details;
    }

    public void setChannel_closed_details(ChannelClosedDetailsBean channel_closed_details) {
        this.channel_closed_details = channel_closed_details;
    }

    public int getQueue_declared() {
        return queue_declared;
    }

    public void setQueue_declared(int queue_declared) {
        this.queue_declared = queue_declared;
    }

    public QueueDeclaredDetailsBean getQueue_declared_details() {
        return queue_declared_details;
    }

    public void setQueue_declared_details(QueueDeclaredDetailsBean queue_declared_details) {
        this.queue_declared_details = queue_declared_details;
    }

    public int getQueue_created() {
        return queue_created;
    }

    public void setQueue_created(int queue_created) {
        this.queue_created = queue_created;
    }

    public QueueCreatedDetailsBean getQueue_created_details() {
        return queue_created_details;
    }

    public void setQueue_created_details(QueueCreatedDetailsBean queue_created_details) {
        this.queue_created_details = queue_created_details;
    }

    public int getQueue_deleted() {
        return queue_deleted;
    }

    public void setQueue_deleted(int queue_deleted) {
        this.queue_deleted = queue_deleted;
    }

    public QueueDeletedDetailsBean getQueue_deleted_details() {
        return queue_deleted_details;
    }

    public void setQueue_deleted_details(QueueDeletedDetailsBean queue_deleted_details) {
        this.queue_deleted_details = queue_deleted_details;
    }

    public MetricsGcQueueLengthBean getMetrics_gc_queue_length() {
        return metrics_gc_queue_length;
    }

    public void setMetrics_gc_queue_length(MetricsGcQueueLengthBean metrics_gc_queue_length) {
        this.metrics_gc_queue_length = metrics_gc_queue_length;
    }

    public List<?> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<?> partitions) {
        this.partitions = partitions;
    }

    public List<ExchangeTypesBean> getExchange_types() {
        return exchange_types;
    }

    public void setExchange_types(List<ExchangeTypesBean> exchange_types) {
        this.exchange_types = exchange_types;
    }

    public List<AuthMechanismsBean> getAuth_mechanisms() {
        return auth_mechanisms;
    }

    public void setAuth_mechanisms(List<AuthMechanismsBean> auth_mechanisms) {
        this.auth_mechanisms = auth_mechanisms;
    }

    public List<ApplicationsBean> getApplications() {
        return applications;
    }

    public void setApplications(List<ApplicationsBean> applications) {
        this.applications = applications;
    }

    public List<ContextsBean> getContexts() {
        return contexts;
    }

    public void setContexts(List<ContextsBean> contexts) {
        this.contexts = contexts;
    }

    public List<String> getLog_files() {
        return log_files;
    }

    public void setLog_files(List<String> log_files) {
        this.log_files = log_files;
    }

    public List<String> getConfig_files() {
        return config_files;
    }

    public void setConfig_files(List<String> config_files) {
        this.config_files = config_files;
    }

    public List<String> getEnabled_plugins() {
        return enabled_plugins;
    }

    public void setEnabled_plugins(List<String> enabled_plugins) {
        this.enabled_plugins = enabled_plugins;
    }

    public List<?> getCluster_links() {
        return cluster_links;
    }

    public void setCluster_links(List<?> cluster_links) {
        this.cluster_links = cluster_links;
    }

    public static class MemUsedDetailsBean {
        /**
         * rate : -2542796.8
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class FdUsedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class SocketsUsedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class ProcUsedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class DiskFreeDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class GcNumDetailsBean {
        /**
         * rate : 509.6
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class GcBytesReclaimedDetailsBean {
        /**
         * rate : 2.34351632E7
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class ContextSwitchesDetailsBean {
        /**
         * rate : 8296.8
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoReadCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoReadBytesDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoReadAvgTimeDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoWriteCountDetailsBean {
        /**
         * rate : 0.6
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoWriteBytesDetailsBean {
        /**
         * rate : 612.8
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoWriteAvgTimeDetailsBean {
        /**
         * rate : 0.08966666666666667
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoSyncCountDetailsBean {
        /**
         * rate : 0.6
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoSyncAvgTimeDetailsBean {
        /**
         * rate : 1.7743333333333333
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoSeekCountDetailsBean {
        /**
         * rate : 0.6
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoSeekAvgTimeDetailsBean {
        /**
         * rate : 0.019
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoReopenCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MnesiaRamTxCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MnesiaDiskTxCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MsgStoreReadCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MsgStoreWriteCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class QueueIndexJournalWriteCountDetailsBean {
        /**
         * rate : 1.2
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class QueueIndexWriteCountDetailsBean {
        /**
         * rate : 0.4
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class QueueIndexReadCountDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoFileHandleOpenAttemptCountDetailsBean {
        /**
         * rate : 2.6
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class IoFileHandleOpenAttemptAvgTimeDetailsBean {
        /**
         * rate : 0.015769230769230768
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class ConnectionCreatedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class ConnectionClosedDetailsBean {
        /**
         * rate : 1.6
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class ChannelCreatedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class ChannelClosedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class QueueDeclaredDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class QueueCreatedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class QueueDeletedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MetricsGcQueueLengthBean {
        /**
         * connection_closed : 0
         * channel_closed : 0
         * consumer_deleted : 0
         * exchange_deleted : 0
         * queue_deleted : 0
         * vhost_deleted : 0
         * node_node_deleted : 0
         * channel_consumer_deleted : 0
         */

        private int connection_closed;
        private int channel_closed;
        private int consumer_deleted;
        private int exchange_deleted;
        private int queue_deleted;
        private int vhost_deleted;
        private int node_node_deleted;
        private int channel_consumer_deleted;

        public int getConnection_closed() {
            return connection_closed;
        }

        public void setConnection_closed(int connection_closed) {
            this.connection_closed = connection_closed;
        }

        public int getChannel_closed() {
            return channel_closed;
        }

        public void setChannel_closed(int channel_closed) {
            this.channel_closed = channel_closed;
        }

        public int getConsumer_deleted() {
            return consumer_deleted;
        }

        public void setConsumer_deleted(int consumer_deleted) {
            this.consumer_deleted = consumer_deleted;
        }

        public int getExchange_deleted() {
            return exchange_deleted;
        }

        public void setExchange_deleted(int exchange_deleted) {
            this.exchange_deleted = exchange_deleted;
        }

        public int getQueue_deleted() {
            return queue_deleted;
        }

        public void setQueue_deleted(int queue_deleted) {
            this.queue_deleted = queue_deleted;
        }

        public int getVhost_deleted() {
            return vhost_deleted;
        }

        public void setVhost_deleted(int vhost_deleted) {
            this.vhost_deleted = vhost_deleted;
        }

        public int getNode_node_deleted() {
            return node_node_deleted;
        }

        public void setNode_node_deleted(int node_node_deleted) {
            this.node_node_deleted = node_node_deleted;
        }

        public int getChannel_consumer_deleted() {
            return channel_consumer_deleted;
        }

        public void setChannel_consumer_deleted(int channel_consumer_deleted) {
            this.channel_consumer_deleted = channel_consumer_deleted;
        }
    }

    public static class ExchangeTypesBean {
        /**
         * name : direct
         * description : AMQP direct exchange, as per the AMQP specification
         * enabled : true
         */

        private String name;
        private String description;
        private boolean enabled;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class AuthMechanismsBean {
        /**
         * name : RABBIT-CR-DEMO
         * description : RabbitMQ Demo challenge-response authentication mechanism
         * enabled : false
         */

        private String name;
        private String description;
        private boolean enabled;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class ApplicationsBean {
        /**
         * name : accept
         * description : Accept header(s) for Erlang/Elixir
         * version : 0.3.3
         */

        private String name;
        private String description;
        private String version;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    public static class ContextsBean {
        /**
         * description : RabbitMQ Management
         * path : /
         * cowboy_opts : [{sendfile,false}]
         * ip : 10.70.20.123
         * port : 15672
         * ssl_opts : {"keyfile":"/opt/dms/maintain/cert/dms.key","certfile":"/opt/dms/maintain/cert/dms.crt","cacertfile":"/opt/dms/maintain/cert/ca.crt"}
         */

        private String description;
        private String path;
        private String cowboy_opts;
        private String ip;
        private String port;
        private SslOptsBean ssl_opts;

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getCowboy_opts() {
            return cowboy_opts;
        }

        public void setCowboy_opts(String cowboy_opts) {
            this.cowboy_opts = cowboy_opts;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public SslOptsBean getSsl_opts() {
            return ssl_opts;
        }

        public void setSsl_opts(SslOptsBean ssl_opts) {
            this.ssl_opts = ssl_opts;
        }

        public static class SslOptsBean {
            /**
             * keyfile : /opt/dms/maintain/cert/dms.key
             * certfile : /opt/dms/maintain/cert/dms.crt
             * cacertfile : /opt/dms/maintain/cert/ca.crt
             */

            private String keyfile;
            private String certfile;
            private String cacertfile;

            public String getKeyfile() {
                return keyfile;
            }

            public void setKeyfile(String keyfile) {
                this.keyfile = keyfile;
            }

            public String getCertfile() {
                return certfile;
            }

            public void setCertfile(String certfile) {
                this.certfile = certfile;
            }

            public String getCacertfile() {
                return cacertfile;
            }

            public void setCacertfile(String cacertfile) {
                this.cacertfile = cacertfile;
            }
        }
    }
}
