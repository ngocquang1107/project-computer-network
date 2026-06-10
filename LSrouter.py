from packet import Packet
from router import Router
import json
import networkx as nx

class LSrouter(Router):
    """
    Giao thức định tuyến Link State (Trạng thái liên kết).
    Luồng hoạt động:
    1. Khởi tạo: Xây dựng cơ sở dữ liệu trạng thái liên kết (LSDB) cục bộ.
    2. Cập nhật (Flooding): Khi nhận được bản tin định tuyến (LSP) chứa trạng thái liên kết của node khác, 
       sử dụng số thứ tự (sequence number) để tránh lặp. Nếu là bản tin mới, cập nhật LSDB và lan truyền tiếp.
    3. Tính toán: Sau mỗi lần LSDB thay đổi, dùng thuật toán Dijkstra xây dựng lại cây đường đi ngắn nhất 
       để cập nhật bảng định tuyến (forwarding_table).
    """
    def __init__(self, addr, heartbeat_time):
        super().__init__(addr)
        self.heartbeat_time = heartbeat_time
        self.last_time = 0 
        
        # Link State Database (LSDB): Lưu trạng thái kết nối của toàn mạng {router_id: {neighbor_id: cost}}
        self.link_state = {self.addr: {}}
        # Lưu số thứ tự (sequence number) gói tin mới nhất từ mỗi router để chống lặp
        self.sequence_numbers = {self.addr: 0}
        
        # Đồ thị topology mạng dùng thư viện networkx
        self.graph = nx.Graph()
        # Bảng định tuyến cục bộ: {đích_đến: (cổng_chuyển_tiếp, cấu_hình_trọng_số)}
        self.forwarding_table = {}

    def handle_packet(self, port, packet):
        """Xử lý gói tin đến: Gói định tuyến (LSP) hoặc gói tin dữ liệu (traceroute)"""
        if packet.kind == Packet.ROUTING:
            # Xử lý thông điệp trạng thái liên kết (LSP - Link State Packet)
            received = json.loads(packet.content)
            src = received["src"]
            seq_num = received["seq_num"]
            neighbors = received["neighbors"]
            
            # Chỉ xử lý thông tin nếu Seq num lớn hơn (tức là thông tin mới hơn)
            if seq_num > self.sequence_numbers.get(src, -1):
                self.sequence_numbers[src] = seq_num
                self.link_state[src] = neighbors
                self.update_graph() # Tính toán lại bảng định tuyến
                self.flood(packet.content, exclude_port=port) # Lan truyền tiếp thông tin này
        
        elif packet.is_traceroute:
            # Xử lý gói tin dữ liệu: Tra bảng định tuyến và chuyển tiếp (forwarding) ra next_hop
            next_hop = self.forwarding_table.get(packet.dst_addr)
            if next_hop:
                self.send(next_hop[0], packet)

    def handle_new_link(self, port, endpoint, cost):
        """Thêm kết nối mới, tăng seq_num và phát tán trạng thái hiện tại"""
        self.link_state.setdefault(self.addr, {})[endpoint] = cost
        self.link_state.setdefault(endpoint, {})[self.addr] = cost
        self.sequence_numbers[self.addr] += 1
        self.broadcast_link_state()

    def handle_remove_link(self, port):
        """Đứt kết nối, xóa thông tin hàng xóm tương ứng, sau đó phát tán nội dung cập nhật"""
        neighbors = self.link_state.get(self.addr, {})
        for neighbor in list(neighbors):
            if self.get_port_for_neighbor(neighbor) is None:
                del neighbors[neighbor]
                if neighbor in self.link_state:
                    self.link_state[neighbor].pop(self.addr, None)
        self.sequence_numbers[self.addr] += 1
        self.update_graph()
        self.broadcast_link_state()

    def handle_time(self, time_ms):
        """Định kỳ tạo gói thông tin trạng thái các liên kết (LSP) của bản thân và gửi đi"""
        if time_ms - self.last_time >= self.heartbeat_time:
            self.last_time = time_ms
            self.sequence_numbers[self.addr] += 1
            self.broadcast_link_state()

    def update_graph(self):
        """Xây dựng lại bản đồ topology của mạng dựa trên toàn bộ csdl LSDB"""
        self.graph.clear()
        for router, neighbors in self.link_state.items():
            for neighbor, cost in neighbors.items():
                self.graph.add_edge(router, neighbor, weight=cost)
        self.update_forwarding_table()

    def update_forwarding_table(self):
        """Chạy thuật toán Dijkstra để tìm đường đi ngắn nhất và cập nhật bảng định tuyến"""
        self.forwarding_table.clear()
        try:
            # Dijkstra tìm đường đi biểu diễn theo kiểu dict với key là node đích và value là list các trạm phải qua
            paths = nx.single_source_dijkstra_path(self.graph, self.addr)
            for dest, path in paths.items():
                if len(path) < 2:
                    continue # Route đến chính nó
                
                next_hop = path[1] # Trạm tiếp theo phải đi
                port = self.get_port_for_neighbor(next_hop)
                cost = self.graph[self.addr][next_hop]["weight"]
                
                if port is not None:
                    self.forwarding_table[dest] = (port, cost)
        except nx.NetworkXNoPath:
            pass

    def broadcast_link_state(self, content=None):
        """Đóng gói trạng thái lân cận của bản thân và gửi (broadcast) tới TẤT CẢ các hàng xóm trực tiếp"""
        if content is None:
            info = {
                "src": self.addr,
                "seq_num": self.sequence_numbers[self.addr],
                "neighbors": self.link_state.get(self.addr, {})
            }
            content = json.dumps(info)
            
        for neighbor in self.link_state.get(self.addr, {}):
            port = self.get_port_for_neighbor(neighbor)
            if port is not None:
                pkt = Packet(kind=Packet.ROUTING, src_addr=self.addr,
                             dst_addr=neighbor, content=content)
                self.send(port, pkt)

    def flood(self, content, exclude_port):
        """Lan truyền (Flooding) bản tin LSP nhận được tới mọi hàng xóm LUÔN trừ cổng vừa nhận tin (để chống loop)"""
        for neighbor in self.link_state.get(self.addr, {}):
            port = self.get_port_for_neighbor(neighbor)
            if port is not None and port != exclude_port:
                pkt = Packet(kind=Packet.ROUTING, src_addr=self.addr,
                             dst_addr=neighbor, content=content)
                self.send(port, pkt)

    def get_port_for_neighbor(self, neighbor):
        """Hàm trợ giúp : Tìm port tương ứng với 1 lân cận"""
        for port, link in self.links.items():
            if link.e1 == neighbor or link.e2 == neighbor:
                return port
        return None

    def __repr__(self):
        return f"LSrouter(addr={self.addr}, LSDB={self.link_state})"