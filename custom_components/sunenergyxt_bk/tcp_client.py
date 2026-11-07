import asyncio
import json
import logging
import queue
import time
from datetime import datetime

from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import *
from .data_info import *

_LOGGER = logging.getLogger(__name__)


class TcpClient:
    """tcp客户端连接处理类."""

    def __init__(self, hass, entry) -> None:
        """tcp初始化函数."""
        self.hass = hass
        self.host = entry.data["host"]
        self.port = entry.data["port"]
        self.entry_id = entry.entry_id
        self.serial_number = entry.data["serial_number"]
        self.reader = None
        self.writer = None
        self.listen_data_task = None
        self.check_connect_task = None
        self.connected = False
        self.checked = False
        self.delay = 5
        self.data = queue.Queue()  # 存储从服务器接收的数据
        self.last_time = datetime.now()
        self._buffer = ""

    async def async_connect(self):
        """建立TCP连接并启动数据监听任务."""
        try:
            # 如果已有连接，先断开
            await self.async_disconnect()

            # 启动连接检查任务
            self.checked = True

            # 失败后尝试连接不需要重复开启监听
            if self.check_connect_task is None:
                self.check_connect_task = asyncio.create_task(
                    self.async_check_connect(), name="tcp_check_connect_task"
                )

            # 建立TCP连接,需要超时机制
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=2.0
            )

            self.connected = True
            self.last_time = datetime.now()
            self._buffer = ""

            await self.async_update_diagnostics("connected")

            # 启动数据接收任务
            self.listen_data_task = asyncio.create_task(
                self.async_listen_data(), name="tcp_listen_data_task"
            )

            _LOGGER.info("Connected to %s:%s", self.host, self.port)

        except Exception as e:
            _LOGGER.debug("connect to %s:%s failed: %s", self.host, self.port, e)
            await self.async_disconnect()

    async def async_disconnect(self):
        """断开TCP连接."""
        self.connected = False
        self._buffer = ""

        await self.async_update_diagnostics("connected")

        # 取消监听任务
        if self.listen_data_task is not None and not self.listen_data_task.done():
            try:
                self.listen_data_task.cancel()
                await asyncio.wait_for(self.listen_data_task, timeout=self.delay)
            except (asyncio.CancelledError, TimeoutError):
                pass  # 需要捕获取消错误,无法转化为异常
            except Exception as e:
                _LOGGER.error("async_disconnect_listen_data_task_error: %s", e)
            finally:
                self.listen_data_task = None

        # 关闭TCP连接
        if self.writer is not None:
            try:
                self.writer.close()
                await asyncio.wait_for(self.writer.wait_closed(), timeout=self.delay)
            except (TimeoutError, Exception) as e:
                _LOGGER.error("async_disconnect_writer_error: %s", e)
            finally:
                self.writer = None
                self.reader = None

    async def async_send_data(self, data: str) -> bool:
        """TCP发送数据函数."""
        if not self.connected or not self.writer:
            _LOGGER.error("not connected, cannot send data")
            return False

        try:
            ascii_bytes = data.encode("ascii", errors="ignore")
            _LOGGER.debug("%s send_data: %s", self.serial_number, ascii_bytes)

            self.writer.write(ascii_bytes)
            await self.writer.drain()
        except Exception as e:
            _LOGGER.error("async_send_data_error: %s", e)
            self.connected = False
            return False

        return True

    async def async_set_data(
        self, data_type: str, data_value: int, set_data: str
    ) -> bool:
        """TCP数据设置函数."""
        # 清除原先存储的数据
        self.data.queue.clear()

        result = await self.async_send_data(set_data)
        if not result:
            return False

        timeout = 2
        start_time = time.monotonic()

        while True:
            if time.monotonic() - start_time > timeout:
                break

            try:
                while not self.data.empty():
                    respond_info = self.data.get()
                    respond_code = getattr(respond_info, "code", None)
                    respond_data = getattr(respond_info, "data", None)

                    respond_value = None
                    if respond_data is not None:
                        respond_value = getattr(respond_data, data_type, None)

                    if respond_code is not None and respond_value is not None:
                        # 返回值为0为成功
                        if respond_code == 0x6057 and respond_value == 0:
                            return True

            except Exception as e:
                _LOGGER.error("async_set_data_error: %s", e)

            await asyncio.sleep(0.1)

        return False

    async def async_listen_data(self):
        """TCP数据监听函数."""
        json_decoder = json.JSONDecoder()
        cancelled = False

        # 持续监听来自TCP服务器的数据
        while self.connected:
            try:
                if self.reader is None:
                    break

                data = await self.reader.read(2048)

                if not data:
                    _LOGGER.info("%s disconnected", self.serial_number)
                    break  # 连接已关闭

                _LOGGER.debug("%s receive_data: %s", self.serial_number, data)

                # 解析数据流，支持流式分包/合包
                decoded_data = data.decode("ascii", errors="ignore")
                self._buffer += decoded_data

                while self._buffer:
                    # 查找 JSON 开始位置
                    start_pos = self._buffer.find('{"code"')
                    if start_pos == -1:
                        brace_pos = self._buffer.find('{')
                        if brace_pos != -1:
                            self._buffer = self._buffer[brace_pos:]
                        else:
                            self._buffer = ""
                        break

                    if start_pos > 0:
                        self._buffer = self._buffer[start_pos:]

                    try:
                        data_dict, end_pos = json_decoder.raw_decode(self._buffer)
                    except json.JSONDecodeError:
                        # 当前缓冲区中是尚未接收完整的数据包，等待下一次 read
                        if len(self._buffer) > 65536:
                            _LOGGER.warning("TCP buffer overflow (>64KB), resetting buffer")
                            self._buffer = ""
                        break

                    # 截取已解析的 JSON 字符串
                    self._buffer = self._buffer[end_pos:]

                    try:
                        respond_info = RespondInfo.from_dict(data_dict)
                    except Exception as err:
                        _LOGGER.warning("Error parsing response dict: %s", err)
                        continue

                    # 设置响应
                    if respond_info.code == 0x6057:
                        self.data.put(respond_info)

                    # 数据上报
                    if respond_info.code in {0x6052, 0x6055, 0x6060}:
                        # 更新数据刷新时间
                        await self.async_update_diagnostics("reporttime")
                        # 更新网络强度
                        if respond_info.data is not None:
                            await self.async_update_diagnostics(
                                "networkrssi", respond_info.data
                            )
                        # 更新实体信息
                        await self.async_update_entities(respond_info)

                    self.last_time = datetime.now()

            except asyncio.CancelledError:
                cancelled = True
                break
            except Exception as e:
                _LOGGER.error("async_listen_data_error: %s", e)
                self.connected = False
                break

        # Reconnect wird automatisch durch async_check_connect im Hintergrund verwaltet

    async def async_check_connect(self):
        """TCP连接状态检查函数."""
        while self.checked:
            try:
                # 判断超时后重新进行连接
                current_time = datetime.now()
                time_diff = abs((current_time - self.last_time).total_seconds())

                if time_diff > 60 and self.connected:
                    _LOGGER.info("No data received from %s:%s in 60s, reconnecting", self.host, self.port)
                    await self.async_disconnect()
                    self.last_time = datetime.now()

                await self.async_update_diagnostics("connected")

                if not self.connected:
                    await self.async_connect()

                # 已连接时5秒检查一次，未连接重试时10秒检查一次
                sleep_time = self.delay if self.connected else 10
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOGGER.error("async_check_connect_error: %s", e)
                self.connected = False
                await asyncio.sleep(self.delay)

    async def async_update_entities(self, data):
        """传感器实体更新状态函数."""
        async_dispatcher_send(
            self.hass,
            f"data_update_{self.entry_id}",
            data,
        )

    async def async_update_diagnostics(self, flag, respond_data=None):
        """传感器诊断实体更新函数."""
        diagnostics_info = None

        if flag == "connected":
            connect_state = "connected" if self.connected else "disconnected"
            diagnostics_info = DiagnosticInfo(connection=connect_state)

        elif flag == "networkrssi":
            rssi = getattr(respond_data, "t475", None)
            if (rssi is not None) and (rssi != 0xFFFFFFFF):
                rssi_data = f"-{rssi} dB"
            else:
                rssi_data = ""
            diagnostics_info = DiagnosticInfo(networkrssi=rssi_data)

        elif flag == "reporttime":
            report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            diagnostics_info = DiagnosticInfo(reporttime=report_time)

        if diagnostics_info is not None:
            async_dispatcher_send(
                self.hass,
                f"diagnostic_update_{self.entry_id}",
                diagnostics_info,
            )



    async def async_stop_client(self):
        """TCP客户端停止函数."""
        self.checked = False

        # 取消状态检查任务
        if self.check_connect_task is not None and not self.check_connect_task.done():
            try:
                self.check_connect_task.cancel()
                await asyncio.wait_for(self.check_connect_task, timeout=self.delay)
            except (asyncio.CancelledError, TimeoutError):
                pass
            except Exception as e:
                _LOGGER.error("async_stop_client_error: %s", e)
            finally:
                self.check_connect_task = None

        await self.async_disconnect()

    async def async_parse_data(self, data) -> RespondInfo | None:
        """TCP客户端数据解析函数."""
        try:
            return RespondInfo.json_to_respond(data)
        except Exception as e:
            _LOGGER.error("async_listen_parse_data_error: %s", e)
            return None

    # Alias for backward compatibility
    async_prase_data = async_parse_data
