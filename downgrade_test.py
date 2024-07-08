import pytest
import logging
import os
import subprocess
from cassandra import ConsistencyLevel
import time

from dtest import Tester
from tools.assertions import  assert_one, assert_all
from ccmlib import common as ccmcommon
import faulthandler

since = pytest.mark.since
logger = logging.getLogger(__name__)

#@pytest.mark.upgrade_test
#@since('4.0', max_version='5.99')
class TestDonwgradeTool(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
             # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        )

    #@pytest.mark.no_offheap_memtables
    def test_downgrade_table(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) """)

        session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")

        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)

            target_version = "github:apache/cassandra-4.1.5"

            #self.downgrade(node, "github:apache/cassandra-4.1")
            self.downgrade(node, target_version, "ks","cf1")
            self.downgrade(node, target_version, "system_auth","cidr_groups")
            self.downgrade(node, target_version, "system_auth","cidr_permissions")
            self.downgrade(node, target_version, "system_auth","identity_to_role")
            self.downgrade(node, target_version, "system_auth","network_permissions")
            self.downgrade(node, target_version, "system_auth","resource_role_permissons_index")
            self.downgrade(node, target_version, "system_auth","role_members")
            self.downgrade(node, target_version, "system_auth","role_permissions")
            self.downgrade(node, target_version, "system_auth","roles")
            self.downgrade(node, target_version, "system_schema","aggregates")
            self.downgrade(node, target_version, "system_schema","column_masks")
            self.downgrade(node, target_version, "system_schema","columns")
            self.downgrade(node, target_version, "system_schema","dropped_columns")
            self.downgrade(node, target_version, "system_schema","functions")
            self.downgrade(node, target_version, "system_schema","indexes")
            self.downgrade(node, target_version, "system_schema","keyspaces")
            self.downgrade(node, target_version, "system_schema","tables")
            self.downgrade(node, target_version, "system_schema","triggers")
            self.downgrade(node, target_version, "system_schema","types")
            self.downgrade(node, target_version, "system_schema","views")
            self.downgrade(node, target_version, "system_distributed","parent_repair_history")
            self.downgrade(node, target_version, "system_distributed","partition_denylist")
            self.downgrade(node, target_version, "system_distributed","repair_history")
            self.downgrade(node, target_version, "system_distributed","view_build_status")
            self.downgrade(node, target_version, "system","IndexInfo")
            self.downgrade(node, target_version, "system","available_ranges")
            self.downgrade(node, target_version, "system","available_ranges_v2")
            self.downgrade(node, target_version, "system","batches")
            self.downgrade(node, target_version, "system","built_views")
            self.downgrade(node, target_version, "system","compaction_history")
            self.downgrade(node, target_version, "system","local")
            self.downgrade(node, target_version, "system","paxos")
            self.downgrade(node, target_version, "system","paxos_repair_history")
            self.downgrade(node, target_version, "system","peer_events")
            self.downgrade(node, target_version, "system","peer_events_v2")
            self.downgrade(node, target_version, "system","peers")
            self.downgrade(node, target_version, "system","peers_v2")
            self.downgrade(node, target_version, "system","prepared_statements")
            self.downgrade(node, target_version, "system","repairs")
            self.downgrade(node, target_version, "system","size_estimates")
            self.downgrade(node, target_version, "system","sstable_activity")
            self.downgrade(node, target_version, "system","sstable_activity_v2")
            self.downgrade(node, target_version, "system","table_estimates")
            self.downgrade(node, target_version, "system","top_partitions")
            self.downgrade(node, target_version, "system","transferred_ranges")
            self.downgrade(node, target_version, "system","transferred_ranges_v2")
            self.downgrade(node, target_version, "system","view_builds_in_progress")
            self.downgrade(node, target_version, "system_traces","events")
            self.downgrade(node, target_version, "system_traces","sessions")
            
            self.set_node_to_current_version(node, target_version)
            node.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)

        assert_all(session, "SELECT * FROM ks.cf1", [[0, 0], [1, 0]],
                   cl=ConsistencyLevel.ALL, ignore_order=True)

        print(self.get_node_versions())


    def downgrade(self, node, tag, keyspace=None, table=None):
        format_args = {'node': node.name, 'tag': tag}
        logger.debug('Downgrading node {node} to nb sstables'.format(**format_args))
        self.install_legacy_parsing(node)



        #node.set_configuration_options(values={'storage_compatibility_mode': 'UPGRADING'})


        files = os.listdir(node.get_conf_dir())
        print("fileeeeeeeeeeeeeeeeeeeeeeeeeessssssssssssssssssss")
        print(files)



        self.update_compatibility_mode(node, "UPGRADING")

        with open(os.path.join(node.get_conf_dir(), 'cassandra.yaml'), 'r') as file:
            lines = file.readlines()
            print("linesss")
            print(lines)
        #time.sleep(5)
        logger.debug('{node} stopped'.format(**format_args))

        logger.debug('Running sstabledowngrade')

        cdir = node.get_install_dir()
        print("cdirrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
        print(cdir)
        env = ccmcommon.make_cassandra_env(cdir, node.get_path())



        data_folder = os.path.join(node.get_path(), 'data')
        os.environ['cassandra_storagedir'] = data_folder

        cmd_args = [node.get_tool('sstabledowngradesingle'), keyspace, table]
        p = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
        stdout, stderr = p.communicate()
        exit_status = p.returncode
        logger.info('stdout: {out}'.format(out=stdout.decode("utf-8")))
        logger.info('stderr: {err}'.format(err=stderr.decode("utf-8")))
        #print("ahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
        print(stdout.decode("utf-8"))
        print(stderr.decode("utf-8"))
        assert 0 == exit_status, \
            "Downgrade complete"
        print("================================++++> Downgrade complete")
        #time.sleep(5)



        faulthandler.enable()

        logger.debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))
        #pytest.set_trace()
        #node.set_install_dir(version=tag, verbose=True)

        # Restart node on new version
        logger.debug('Starting {node} on new version ({tag})'.format(**format_args))
        print("starttttttttttttttttttttttttttttttttttttttttttttttt")
        print(node)



        self.remove_lines_with_substring(os.path.join(node.get_conf_dir(), 'cassandra.yaml'), 'storage_compatibility_mode')


        #node.start(wait_for_binary_proto=True)

    def stop_node(self, node):
        node.flush()
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False, gently=True)

    def set_node_to_current_version(self, node, tag):
        return node.set_install_dir(version=tag, verbose=True)

    def get_node_versions(self):
        return [n.get_cassandra_version() for n in self.cluster.nodelist()]


    def remove_lines_with_substring(self, filename, substring):
        # Read the contents of the file
        with open(filename, 'r') as file:
            lines = file.readlines()

        # Filter out lines containing the specified substring
        filtered_lines = [line for line in lines if substring not in line]

        # Write the filtered lines back to the file
        with open(filename, 'w') as file:
            file.writelines(filtered_lines)


    def update_compatibility_mode(self, node, mode):
        cassandra_config_path = os.path.join(node.get_conf_dir(), 'cassandra.yaml')
        with open(cassandra_config_path, 'r') as file:
            lines = file.readlines()
            lines = [line for line in lines if "compatibility_mode" not in line]
        with open(cassandra_config_path, 'w') as snitch_file:
            snitch_file.write("storage_compatibility_mode: " + mode + os.linesep)
            snitch_file.writelines(lines)



        #assert False

        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])


        # # Create table
        # session = self.patient_cql_connection(node1)
        # session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        # session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) WITH compression = {
        #                   'sstable_compression': 'DeflateCompressor',
        #                   'chunk_length_kb': 256,
        #                   'crc_check_chance': 0.6 }
        #                 """)
        #
        # # Insert and query data
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (2, 0)")
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (3, 0)")
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=2", [2, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=3", [3, 0])
        # session.shutdown()
        #
        # self.verify_old_crc_check_chance(node1)
        # self.verify_old_crc_check_chance(node2)
        #
        # # upgrade node1 to 3.0
        # self.upgrade_to_version("cassandra-3.0", node1)
        #
        # self.verify_new_crc_check_chance(node1)
        # self.verify_old_crc_check_chance(node2)
        #
        # # Insert and query data
        # session = self.patient_cql_connection(node1)
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (4, 0)")
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (5, 0)")
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (6, 0)")
        # session.execute("INSERT INTO ks.cf1(id, val) VALUES (7, 0)")
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=2", [2, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=3", [3, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=4", [4, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=5", [5, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=6", [6, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=7", [7, 0])
        # session.shutdown()
        #
        # # upgrade node2 to 3.0
        # self.upgrade_to_version("cassandra-3.0", node2)
        #
        # self.verify_new_crc_check_chance(node1)
        # self.verify_new_crc_check_chance(node2)
        #
        # # read data again
        # session = self.patient_cql_connection(node1)
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=2", [2, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=3", [3, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=4", [4, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=5", [5, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=6", [6, 0])
        # assert_one(session, "SELECT * FROM ks.cf1 WHERE id=7", [7, 0])
        # session.shutdown()
        #
        # logger.debug('Test completed successfully')

    # def verify_old_crc_check_chance(self, node):
    #     session = self.patient_exclusive_cql_connection(node)
    #     session.cluster.refresh_schema_metadata(0)
    #     meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
    #     logger.debug(meta.options['compression_parameters'])
    #     assert '{"crc_check_chance":"0.6","sstable_compression":"org.apache.cassandra.io.compress.DeflateCompressor","chunk_length_kb":"256"}' \
    #            == meta.options['compression_parameters']
    #     session.shutdown()
    #
    # def verify_new_crc_check_chance(self, node):
    #     session = self.patient_exclusive_cql_connection(node)
    #     session.cluster.refresh_schema_metadata(0)
    #     meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
    #     assert 'org.apache.cassandra.io.compress.DeflateCompressor' == meta.options['compression']['class']
    #     assert '256' == meta.options['compression']['chunk_length_in_kb']
    #     assert_crc_check_chance_equal(session, "cf1", 0.6)
    #     session.shutdown()
    #
    # def upgrade_to_version(self, tag, node):
    #     format_args = {'node': node.name, 'tag': tag}
    #     logger.debug('Upgrading node {node} to {tag}'.format(**format_args))
    #     self.install_legacy_parsing(node)
    #     # drain and shutdown
    #     node.drain()
    #     node.watch_log_for("DRAINED")
    #     node.stop(wait_other_notice=False)
    #     logger.debug('{node} stopped'.format(**format_args))
    #
    #     # Update Cassandra Directory
    #     logger.debug('Updating version to tag {tag}'.format(**format_args))
    #
    #     logger.debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))
    #     node.set_install_dir(version='git:' + tag, verbose=True)
    #     self.install_legacy_parsing(node)
    #     # Restart node on new version
    #     logger.debug('Starting {node} on new version ({tag})'.format(**format_args))
    #     # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
    #     node.set_log_level("INFO")
    #     node.start(wait_for_binary_proto=True, jvm_args=['-Dcassandra.disable_max_protocol_auto_override=true'])
    #
    #     logger.debug('Running upgradesstables')
    #     node.nodetool('upgradesstables -a')
    #     logger.debug('Upgrade of {node} complete'.format(**format_args))