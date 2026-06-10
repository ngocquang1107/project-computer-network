from packet import Packet
from router import Router
import json
import networkx as nx

class LSrouter(Router):
    """
    Giao thức định tuyến Link State (Trạng thái liên kết).
    Luồng hoạt động chính:
    1. Khám phá lân cận: Lưu chi phí liên kết trực tiếp với các node xung quanh vào CSDL của router (LSDB).
    2. Flooding (Lũ lụt): Phát tán (broadcast) LSDB ra toàn mạng mỗi khi có sự thay đổi, kết hợp sequence number để luôn dùng bản ghi mới nhất.
    3. Trạm lưu trữ toàn mạng: Thu thập bản tin link-state từ tất cả router khác để vẽ lại bức tranh tổng thể (Đồ thị Graph).
    4. Cập nhật đường đi (Dijkstra): Chạy thuật toán Dijkstra trên Đồ thị vừa tạo ra để tính đường đi ngắn nhất đến khắp các điểm mạng, từ đó cập nhật Bảng chuyển tiếp (Forwarding Table).
    """

    def __init__(self, addr, heartbeat_time):
        super().__init__(addr)  # Khởi tạo lớp cha RouterRouter
        self.heartbeat_time = heartbeat_time    # Thời gian gửi lại thông tin link-state
        self.last_time = 0  # Lưu thời điểm gửi heartbeat gần nhất
        self.link_state = {self.addr: {}}   # CSDL trạng thái liên kết: {router: {neighbor: cost}}
        self.sequence_numbers = {self.addr: 0}  # STT mới nhất của mỗi router
        self.graph = nx.Graph() # Đồ thị để tính đường đi ngắn nhất
        self.forwarding_table = {}  # Bảng forwarding: {destination: (port, cost)}

    def handle_packet(self, port, packet):
        """Process incoming packet."""
        if packet.kind == Packet.ROUTING:
            received = json.loads(packet.content)
            src = received["src"]
            seq_num = received["seq_num"]
            neighbors = received["neighbors"]
            # Nếu seq_num mới hơn hiện tại thì cập nhật CSDL link-state
            if seq_num > self.sequence_numbers.get(src, -1):
                self.sequence_numbers[src] = seq_num
                self.link_state[src] = neighbors
                self.update_graph()
                self.flood(packet.content, exclude_port=port)   # Flood đến các neighbor khác trừ cổng vừa nhận
        elif packet.is_traceroute:
            # Chuyển tiếp traceroute dựa vào bảng forwarding
            next_hop = self.forwarding_table.get(packet.dst_addr)
            if next_hop:
                self.send(next_hop[0], packet)

    def handle_new_link(self, port, endpoint, cost):
        """Handle new link."""
        # Thêm liên kết 2 chiều
        self.link_state.setdefault(self.addr, {})[endpoint] = cost # link_state[A][B] = cost
        self.link_state.setdefault(endpoint, {})[self.addr] = cost  # link_state[B][A] = cost
        self.sequence_numbers[self.addr] += 1
        self.broadcast_link_state() # Gửi thông tin cho các neighbors

    def handle_remove_link(self, port):
        """Handle removed link."""
        # Xóa liên kết 2 chiều khỏi CSDL khi link vật lý bị gỡ
        neighbors = self.link_state.get(self.addr, {})  # Lấy danh sách neighbor
        for neighbor in list(neighbors):
            if self.get_port_for_neighbor(neighbor) is None: # Nếu không có port kết nối đến
                del neighbors[neighbor]
                if neighbor in self.link_state: # Xóa liên kết ngược
                    self.link_state[neighbor].pop(self.addr, None)
        self.sequence_numbers[self.addr] += 1
        self.update_graph()
        self.broadcast_link_state()

    def handle_time(self, time_ms):
        """Periodic heartbeat to rebroadcast own LS."""
        if time_ms - self.last_time >= self.heartbeat_time:
            self.last_time = time_ms    # Cập nhật thời gian gửi heartbeat là hiện tại
            self.sequence_numbers[self.addr] += 1
            self.broadcast_link_state()

    def update_graph(self):
        """Update the network graph based on the current link state."""
        self.graph.clear()
        for router, neighbors in self.link_state.items():
            for neighbor, cost in neighbors.items():
                self.graph.add_edge(router, neighbor, weight=cost)  # Thêm cạnh vào đồ thị
        self.update_forwarding_table()

    def update_forwarding_table(self):
        """Compute shortest paths and build forwarding table."""
        self.forwarding_table.clear()
        try:
            paths = nx.single_source_dijkstra_path(self.graph, self.addr)   # paths là dict: {destination: [self.addr, next_hop, ..., destination]}
            for dest, path in paths.items():
                if len(path) < 2:
                    continue
                next_hop = path[1]  # next_hop là router đầu tiên trên đường đi từ self đến dest
                port = self.get_port_for_neighbor(next_hop)
                cost = self.graph[self.addr][next_hop]["weight"]
                if port is not None:
                    self.forwarding_table[dest] = (port, cost)
        except nx.NetworkXNoPath:
            pass

    def broadcast_link_state(self, content=None):
        """Broadcast the link state of this router to all neighbors."""
        if content is None:
            info = {
                "src": self.addr,
                "seq_num": self.sequence_numbers[self.addr],
                "neighbors": self.link_state.get(self.addr, {})
            }
            content = json.dumps(info)
        for neighbor in self.link_state.get(self.addr, {}): # Gửi cho các neighbors
            port = self.get_port_for_neighbor(neighbor)
            if port is not None:
                pkt = Packet(kind=Packet.ROUTING, src_addr=self.addr,
                             dst_addr=neighbor, content=content)
                self.send(port, pkt)

    def flood(self, content, exclude_port):
        """Forward LS packet to all neighbors except the one it came from."""
        for neighbor in self.link_state.get(self.addr, {}):
            port = self.get_port_for_neighbor(neighbor)
            if port is not None and port != exclude_port: # Không phải port vừa nhận gói tin
                pkt = Packet(kind=Packet.ROUTING, src_addr=self.addr,
                             dst_addr=neighbor, content=content)
                self.send(port, pkt)

    def get_port_for_neighbor(self, neighbor):
        """Get the port for a given neighbor."""
        for port, link in self.links.items():
            if link.e1 == neighbor or link.e2 == neighbor:  # Nếu 1 trong 2 đầu là neighbor
                return port
        return None

    def __repr__(self):
        # Hiển thị mô tả bao gồm địa chỉ router và CSDL trạng thái liên kết
        return f"LSrouter(addr={self.addr}, LSDB={self.link_state})"