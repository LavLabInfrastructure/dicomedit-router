# Standard library imports
import os
import re
import sys
import time
import yaml
import shutil
import asyncio
import logging
from urllib.parse import urlparse, parse_qs, urlencode
from tempfile import TemporaryDirectory
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process, Manager
from threading import current_thread

# Third-party imports
import httpx
import javabridge
from pydicom import dcmread
from pydicom.dataset import validate_file_meta
from pydicom.tag import Tag
from pydicom.filewriter import write_file_meta_info
from pynetdicom import (
    AE,
    evt,
    _globals,
    sop_class,
    StoragePresentationContexts,
    build_context,
)


def create_AE(title):
    ae = AE(ae_title=title)
    ae.requested_contexts = StoragePresentationContexts
    for t_syntax in _globals.ALL_TRANSFER_SYNTAXES:
        for name, uid in sop_class._STORAGE_CLASSES.items():
            ae.add_supported_context(uid, t_syntax, scu_role=True, scp_role=True)

    return ae


def prepare_routes(routes):
    if routes is None:
        logging.error("No routes defined! Closing...")
        sys.exit(1)

    rv = []
    for key in routes:
        route = routes[key]
        ent = create_AE(route.get("local_ae", "LLAB_ROUTER"))
        addr = route.get("address", "localhost")
        port = int(route.get("port", 8104))
        rae = route.get("remote_ae", "ANY_SCP")
        deid = bool(route.get("deidentify", True))
        rv.append(
            {
                "id": key,
                "ent": ent,
                "addr": addr,
                "port": port,
                "rae": rae,
                "deid": deid,
                # 'contexts': contexts
            }
        )

    return rv


def prepare_filters(filters):
    rv = {}
    if filters is None:
        return rv
    for tag in filters:
        group, id = str(tag).split(",")
        key = Tag((int(group), int(id)))
        regex = filters[tag]
        rv.update({key: regex})
    return rv


def compile_cmd(de6_path, de6_script_dir):
    # cmd=['java','-jar', str(de6_path)]
    cmd = []
    # TODO cmd.extend(java_opts)
    files = [f for f in os.listdir(de6_script_dir) if re.match(r"^[0-9]+.*\.das", f)]
    files.sort()
    for file in files:
        das_script = os.path.join(de6_script_dir, file)
        script_id = re.match(r"^[0-9]+-", file).group()
        lookup_tables = [
            f
            for f in os.listdir(de6_script_dir)
            if re.match(script_id + ".*\.properties", f)
        ]
        for f in lookup_tables:
            das_script += f",{os.path.join(de6_script_dir,f)}"
        cmd.extend(["-s", das_script])
    logging.info(f"DicomEdit6 command: {cmd}")
    return cmd


def dcm_download(workdir: TemporaryDirectory, event):
    filename = event.request.AffectedSOPInstanceUID + ".dcm"
    filepath = os.path.join(workdir.name, filename)
    try:
        with open(filepath, "wb") as dcm:
            dcm.write(b"\x00" * 128)
            dcm.write(b"DICM")
            write_file_meta_info(dcm, event.file_meta)
            dcm.write(event.request.DataSet.getvalue())
            logging.info(f"Recieved dicom: {filename}")
        return filepath
    except Exception:
        raise Exception


def dcm_verify(dicom):
    """"""
    dcm_val = dcmread(dicom)
    validate_file_meta(dcm_val.file_meta)
    for tag in filters:
        filter = filters[tag]
        value = dcm_val.get(tag, "")
        if re.match(filter, value) is None:
            raise SystemError(
                f"DICOM WAS REJECTED FOR TAG: {tag} NOT MATCHING REGEX: {filter}"
            )


def prepare_scu_context(dcm):
    dcm_obj = dcmread(dcm, stop_before_pixels=True, specific_tags=[Tag((2, 10))])

    abstract_syntax = dcm_obj.file_meta.MediaStorageSOPClassUID
    transfer_syntax = [dcm_obj.file_meta.TransferSyntaxUID]

    return [build_context(abstract_syntax, transfer_syntax)]


def dcm_deid(input_file: str, output: TemporaryDirectory):
    output_file = output.name + "/DEID.dcm"
    shutil.copy(input_file, output_file)
    cmd = deid_cmd.copy()
    cmd.extend(["-i", str(output_file), "-o", output.name])
    logging.info(cmd)
    ssa.main(cmd)
    # subprocess.run(cmd, stdout=open('/dev/null'))
    return output_file


def dcm_route(phi_dcm, de6_dcm):
    """"""
    for route in routes:
        try:
            if route["deid"] is True:
                dcm = de6_dcm
            else:
                dcm = phi_dcm

            assoc = route["ent"].associate(
                route["addr"],
                route["port"],
                prepare_scu_context(dcm),
                ae_title=route["rae"],
            )
            logging.info(
                f'Routing dcm: {phi_dcm} to configured destination: {route["id"]}'
            )
            if assoc.is_established:
                status = assoc.send_c_store(dcm)
                if status:
                    logging.info(
                        f'C-STORE:{route["id"]} request status: {status.Status}'
                    )
                assoc.release()
            else:
                logging.info(
                    f'Failed to create association for config: {route["id"]} to send dicom: {dcm}'
                )
        except Exception as e:
            logging.error(f"Failed to route dicom: {dcm}! See message below!")
            logging.error(e)


def dcm_process(workdir, dicom):
    """"""
    # start=time.time()
    de6_dcm = dcm_deid(dicom, workdir)
    logging.info(os.listdir(workdir.name))
    dcm_route(dicom, de6_dcm)
    workdir.cleanup()
    logging.info(f"Successfully routed {dicom} and cleaned {workdir}")
    # logging.error(f'Deid and routing took:{time.time()-start}')


def handle_store(event):
    """"""
    workdir = TemporaryDirectory(dir=cache_dir)
    try:
        dicom = dcm_download(workdir, event)
    except Exception as e:
        logging.error("Error while downloading dicom! See error message below")
        logging.error(e)
        return 0xA700
    try:
        dcm_verify(dicom)
    except Exception as e:
        logging.error("Error while verifying dicom! See error message below")
        logging.error(e)
        return 0xC000
    ppe.submit(dcm_process, workdir, dicom)
    return 0x0000


def prepare_jvm():
    """"""
    logging.error(f"PREPARING THREAD:{current_thread()}")
    global ssa
    javabridge.JARS.extend([dicom_edit6_path])
    javabridge.start_vm(run_headless=True)

    system = javabridge.JClassWrapper("java.lang.System")
    stream = javabridge.JClassWrapper("java.io.PrintStream")
    output = javabridge.JClassWrapper("java.io.OutputStream")
    system.setOut(stream(output.nullOutputStream()))

    ssa = javabridge.JClassWrapper("org.nrg.dicom.dicomedit.SerialScriptAnonymizer")


async def cache_process(proxy_url, proxy_port, expiration):
    cache_dict = {}
    active_requests_lock = asyncio.Lock()
    active_requests = {}

    async def handle_client(reader, writer):
        async with active_requests_lock:
            request_data = await reader.read(1024)
            try:
                request_line = request_data.splitlines()[0]
                method, original_url, _ = request_line.decode().split()
            except Exception as e:
                logging.error(f"{request_line.decode()}:{e}")
                writer.close()
                return

            # Parse the URLs
            parsed_proxy_url = urlparse(proxy_url)
            parsed_original_url = urlparse(original_url)

            # Combine the queries
            proxy_queries = parse_qs(parsed_proxy_url.query)
            original_queries = parse_qs(parsed_original_url.query)
            combined_queries = {**proxy_queries, **original_queries}
            combined_query_string = urlencode(combined_queries, doseq=True)

            # Combine the paths
            combined_path = (
                parsed_proxy_url.path.rstrip("/")
                + "/"
                + parsed_original_url.path.lstrip("/")
            )

            # Construct the new URL
            new_url = (
                f"{parsed_proxy_url.scheme}://{parsed_proxy_url.netloc}{combined_path}"
            )
            if combined_query_string:
                new_url += "?" + combined_query_string

            # Check for active requests
            if new_url in active_requests:
                await active_requests[
                    new_url
                ]  # Wait for the original request to complete
            elif (
                new_url in cache_dict
                and (time.time() - cache_dict[new_url]["time"]) < expiration
            ):
                response_content = cache_dict[new_url]["content"]
                response_status = cache_dict[new_url]["status"]
                response_headers = cache_dict[new_url]["headers"]
            else:
                # Create a future for other duplicate requests to await
                active_requests[new_url] = asyncio.Future()
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.request(method, new_url)
                    response_content = response.content
                    response_status = response.status_code
                    response_headers = response.headers
                    response_headers["Content-Type"] = "text/plain"
                    response_headers["Content-Length"] = str(
                        len(response_content.decode())
                    )
                    if "transfer-encoding" in response_headers:
                        response_headers.pop("transfer-encoding")
                    response_headers = response_headers.items()
                # Cache the response
                cache_dict[new_url] = {
                    "time": time.time(),
                    "content": response_content,
                    "status": response_status,
                    "headers": response_headers,
                }
                # Notify duplicate requests that the original has completed
                active_requests[new_url].set_result(True)
                del active_requests[new_url]

            # Construct the HTTP response
            response_line = f"HTTP/1.1 {response_status} OK\r\nContent-Type: text/plain\r\nContent-Length: {len(response_content)}\r\n\r\n"
            # headers = "\r\n".join([f"{k}: {v}" for k, v in response_headers]) + "\r\n\r\n"
            response_data = response_line.encode() + response_content

            # Send the response back to the client
            writer.write(response_data)
            await writer.drain()
            # writer.write_eof()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "0.0.0.0", proxy_port)
    async with server:
        print(f"Proxy server listening on localhost:{proxy_port}")
        await server.serve_forever()


def start_cache_process(cache_url, cache_port, cache_expiration):
    asyncio.run(cache_process(cache_url, cache_port, cache_expiration))


def prepare_caches(cache_yml):
    if cache_yml is None or len(cache_yml) == 0:
        return

    procs = []
    for cache in cache_yml:
        if cache.get("url") is None or cache.get("port") is None:
            logging.error(
                f"Invalid cache config, remote url and local proxy port are required: {cache}"
            )
            continue

        cache_url = cache.get("url")
        cache_port = int(cache.get("port"))
        cache_expiration = int(cache.get("expiration", 43200))

        cache_proc = Process(
            target=start_cache_process, args=(cache_url, cache_port, cache_expiration)
        )
        cache_proc.start()
        procs.append(cache_proc)
    return procs


if __name__ == "__main__":
    # TODO get from env
    cache_dir = None
    dicom_edit6_path = "/opt/dicomedit6.jar"
    anon_script_path = "/config/das"
    config_path = "/config/config.yml"
    local_scp_port = 8104
    max_jobs = 12

    # parse configuration
    yml = None
    with open(config_path, "r") as f:
        yml = yaml.load(f, yaml.CBaseLoader)
    ppe = ProcessPoolExecutor(max_jobs, initializer=prepare_jvm)
    routes = prepare_routes(yml.get("routes"))
    filters = prepare_filters(yml.get("filters"))
    _ = prepare_caches(yml.get("caches"))
    deid_cmd = compile_cmd(dicom_edit6_path, anon_script_path)

    ae = create_AE(yml.get("title", "LLAB_ROUTER"))
    handlers = [(evt.EVT_C_STORE, handle_store)]
    logging.info("Starting router")
    jobs = []
    server = ae.start_server(
        ("0.0.0.0", local_scp_port), evt_handlers=handlers, block=False
    )
    while jobs is not None:
        if len(jobs) > 0:
            while not jobs[0].done():
                time.sleep(0.5)
            jobs.pop(0)
        time.sleep(0.5)
