import os
import time
import socket
import subprocess

from contextlib import closing

from pathlib import Path


def now():
    return time.perf_counter()


import pendulum
from pendulum._extensions.helpers import timestamp


ssh_key_folder = Path.home() / ".ssh"
ssh_key_path = ssh_key_folder / "id_ed25519_snapreader"
ssh_user = "snapreader"
ssh_host = "172.31.42.56"


def get_snapshot_folder():
    sftp = get_sftp_client()
    folders = sftp.listdir("snapshots")
    folder = sorted(folders)[-1]
    return folder


def setup_ssh():
    ssh_key_folder.mkdir(exist_ok=True)
    with ssh_key_path.open("w") as ssh_key_file:
        ssh_key_file.write(os.environ["SYNC_SSH_KEY"])


def setup_rclone():
    setup_ssh()
    subprocess.run(
        f"rclone config create sftp sftp host {ssh_host} user {ssh_user} key_file {ssh_key_path}",
        shell=True,
        check=True,
    )
    subprocess.run(
        f"rclone config create s3 s3 type s3 provider AWS env_auth true region us-east-2",
        shell=True,
        check=True,
    )


def get_sftp_client():
    import paramiko
    setup_ssh()
    pk = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
    transport = paramiko.Transport(ssh_host)
    transport.connect(username=ssh_user, pkey=pk)
    return paramiko.SFTPClient.from_transport(transport)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def set_prometheus_env():
    free_port = find_free_port()
    os.environ["VISOR_PROMETHEUS_PORT"] = f":{free_port}"


def epoc_to_date(epoc):
    timestamp = 1598306400 + 30 * int(epoc)
    return pendulum.from_timestamp(timestamp)


def date_to_epoc(date):
    timestamp = date.int_timestamp
    epoc = (timestamp - 1598306400) / 30
    assert epoc.is_integer()
    return int(epoc)


def get_execution_time():
    airflow_ts = os.environ["AIRFLOW_TS"]
    return pendulum.parse(airflow_ts)


def get_start_end_epocs():
    execution_time = get_execution_time()
    if os.getenv("AIRFLOW_INTERVAL") == "DAILY":
        start_time = execution_time.start_of("day")
        end_time = start_time.add(days=1)
    else:
        start_time = execution_time.subtract(hours=9)
        end_time = execution_time.subtract(hours=8)

    start_epoc = date_to_epoc(start_time)
    end_epoc = date_to_epoc(end_time)
    return start_epoc, end_epoc


def get_dt():
    execution_time = get_execution_time()
    return execution_time.format("YYYY-MM-DD")


def get_output_folder():
    job_name = os.environ["JOB_NAME"]
    csv_base_path = os.getenv("CSV_BASE_PATH", "/output")
    start_epoc, end_epoc = get_start_end_epocs()
    return f"{csv_base_path}/{job_name}/{start_epoc}__{end_epoc}"


tables = {
    "actor_states": "height,head,code,state",
    "actors": "height,id,state_root,code,head,balance,nonce",
    "block_headers": "height,cid,miner,parent_weight,parent_base_fee,parent_state_root,win_count,timestamp,fork_signaling",
    "block_messages": "height,block,message",
    "block_parents": "height,block,parent",
    "chain_economics": "parent_state_root,circulating_fil,vested_fil,mined_fil,burnt_fil,locked_fil",
    "chain_powers": "height,state_root,total_raw_bytes_power,total_qa_bytes_power,total_raw_bytes_committed,total_qa_bytes_committed,total_pledge_collateral,qa_smoothed_position_estimate,qa_smoothed_velocity_estimate,miner_count,participating_miner_count",
    "chain_rewards": "height,state_root,cum_sum_baseline,cum_sum_realized,effective_baseline_power,new_baseline_power,new_reward_smoothed_position_estimate,new_reward_smoothed_velocity_estimate,total_mined_reward,new_reward,effective_network_time",
    "derived_gas_outputs": 'height,cid,state_root,"from","to",value,gas_fee_cap,gas_premium,gas_limit,size_bytes,nonce,method,actor_name,exit_code,gas_used,parent_base_fee,base_fee_burn,over_estimation_burn,miner_penalty,miner_tip,refund,gas_refund,gas_burned',
    "drand_block_entries": "round,block",
    "id_addresses": "id,address,state_root",
    "market_deal_proposals": "height,deal_id,state_root,padded_piece_size,unpadded_piece_size,start_epoch,end_epoch,client_id,provider_id,client_collateral,provider_collateral,storage_price_per_epoch,piece_cid,is_verified,label",
    "market_deal_states": "height,deal_id,sector_start_epoch,last_update_epoch,slash_epoch,state_root",
    "message_gas_economy": "height,state_root,base_fee,base_fee_change_log,gas_limit_total,gas_limit_unique_total,gas_fill_ratio,gas_capacity_ratio,gas_waste_ratio",
    "messages": 'height,cid,"from","to",value,gas_fee_cap,gas_premium,gas_limit,size_bytes,nonce,method',
    "miner_current_deadline_infos": "height,miner_id,state_root,deadline_index,period_start,open,close,challenge,fault_cutoff",
    "miner_infos": "height,miner_id,state_root,owner_id,worker_id,new_worker,worker_change_epoch,consensus_faulted_elapsed,peer_id,control_addresses,multi_addresses",
    "miner_fee_debts": "height,miner_id,state_root,fee_debt",
    "miner_locked_funds": "height,miner_id,state_root,locked_funds,initial_pledge,pre_commit_deposits",
    "miner_pre_commit_infos": "height,miner_id,sector_id,state_root,sealed_cid,seal_rand_epoch,expiration_epoch,pre_commit_deposit,pre_commit_epoch,deal_weight,verified_deal_weight,is_replace_capacity,replace_sector_deadline,replace_sector_partition,replace_sector_number",
    "miner_sector_deals": "height,miner_id,sector_id,deal_id",
    "miner_sector_events": "height,miner_id,sector_id,state_root,event",
    "miner_sector_infos": "height,miner_id,sector_id,state_root,sealed_cid,activation_epoch,expiration_epoch,deal_weight,verified_deal_weight,initial_pledge,expected_day_reward,expected_storage_pledge",
    "miner_sector_posts": "height,miner_id,sector_id,post_message_cid",
    "multisig_transactions": 'multisig_id,state_root,height,transaction_id,"to",value,method,params,approved',
    "multisig_approvals": 'height,state_root,multisig_id,message,method,approver,threshold,initial_balance,signers,gas_used,transaction_id,"to",value',
    "parsed_messages": 'height,cid,"from","to",value,method,params',
    "power_actor_claims": "height,miner_id,state_root,raw_byte_power,quality_adj_power",
    "receipts": "height,message,state_root,idx,exit_code,gas_used",
    "visor_processing_reports": "height,state_root,reporter,task,started_at,completed_at,status,status_information,errors_detected",
}

v1_tables = {
    "chain_economics": "height,parent_state_root,circulating_fil,vested_fil,mined_fil,burnt_fil,locked_fil,fil_reserve_disbursed",
    "derived_gas_outputs": 'height,cid,state_root,"from","to",value,gas_fee_cap,gas_premium,gas_limit,size_bytes,nonce,method,actor_name,actor_family,exit_code,gas_used,parent_base_fee,base_fee_burn,over_estimation_burn,miner_penalty,miner_tip,refund,gas_refund,gas_burned',
    "id_addresses": "height,id,address,state_root",
    "miner_infos": "height,miner_id,state_root,owner_id,worker_id,new_worker,worker_change_epoch,consensus_faulted_elapsed,peer_id,control_addresses,multi_addresses,sector_size",
}


def get_export_filename(table_name):
    csv_base_path = os.getenv("CSV_BASE_PATH", "/output")
    dt = get_dt()
    return f"{csv_base_path}/export/{table_name}/{dt}.csv.gz"


def import_table(connect_str, table_name):
    v1_filename = f"{table_name}.csv"
    v0_filename = f"{table_name}.v0.csv"
    v0_columns = tables[table_name]
    v1_columns = tables[table_name]
    if table_name in v1_tables:
        v1_columns = v1_tables[table_name]
        csv_cut_comand = (
            f"/usr/bin/csvcut -c {v0_columns} {v1_filename} > {v0_filename}"
        )
        print(csv_cut_comand, flush=True)
        subprocess.run(csv_cut_comand, shell=True, check=True)

    if os.getenv("VISOR_SCHEMA_VERSION") == "v0":
        import_filename = v0_filename
        columns = v0_columns
    else:
        import_filename = v1_filename
        columns = v1_columns

    visor_database_schema = os.getenv("VISOR_DATABASE_SCHEMA", "visor")
    if not os.path.isfile(import_filename):
        # raise Exception(f"Could not find {filename} in {os.getcwd()}")
        print(f"Could not find {import_filename} in {os.getcwd()}")
    tmp_table_name = f"backfill_temp_{table_name}"
    # Split into multiple commands because psql requires \copy to be a separate command
    command1 = f"""BEGIN TRANSACTION;CREATE TEMP TABLE {tmp_table_name} ON COMMIT DROP AS SELECT * FROM {visor_database_schema}.{table_name} WITH NO DATA;"""
    command2 = f"""\\copy {tmp_table_name}({columns}) FROM '{import_filename}' DELIMITER ',' CSV HEADER;"""
    command3 = f"""INSERT INTO {visor_database_schema}.{table_name} SELECT * FROM {tmp_table_name} ON CONFLICT DO NOTHING;COMMIT;"""
    print(f"Loading {table_name} from {import_filename}", end=" ", flush=True)
    process = subprocess.run(
        [
            "psql",
            "-q",
            "-a",
            "-X",
            "-t",
            "-d",
            connect_str,
            "-c",
            command1,
            "-c",
            command2,
            "-c",
            command3,
        ],
        capture_output=True,
        text=True,
    )
    if process.returncode == 0:
        print("✓", flush=True)
    else:
        print("❌")
        print(process.stderr)
        print(process.stdout, flush=True)
        raise Exception(f"Could not import {table_name}")
