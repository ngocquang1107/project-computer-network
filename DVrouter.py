import json
from router import Router
from packet import Packet


class DVrouter(Router):
    """
    Giao thức định tuyến Distance Vector (Vector khoảng cách).
    Luồng hoạt động:
    1. Khởi tạo: Duy trì bảng định tuyến của bản thân và lưu trữ vector khoảng cách từ các lân cận.
    2. Lắng nghe/Cập nhật: Nhận bản tin từ lân cận hoặc phát hiện thay đổi liên kết -> Tính lại bảng định tuyến (Bellman-Ford).
    3. Phát tán: Nếu bảng định tuyến thay đổi sau khi tính lại, tự động gửi vector mới cho tất cả các lân cận.
    """
    def __init__(self, addr, heartbeat_time):
        Router.__init__(self, addr)
        self.heartbeat_time = heartbeat_time
        self.last_time = 0

        # Lưu khoảng cách trực tiếp đến lân cận: {neighbor_addr: (cost, port)}
        self.neighbors = {}
        # Lưu bản sao vector khoảng cách của từng lân cận: {neighbor_addr: {dest: cost}}
        self.neighbors_vector = {}
        # Bảng định tuyến cục bộ: {đích_đến: (tổng_chi_phí, cổng_next_hop)}
        self.routing_table = {addr : (0, None)}

    def handle_packet(self, port, packet : Packet):
        """Xử lý gói tin đến: phân loại thành gói dữ liệu mạng hoặc gói thông tin định tuyến."""

        if packet.is_traceroute:
            # Gói tin dữ liệu: Tra cứu bảng định tuyến để tìm cổng chuyển tiếp (forwarding)
            if packet.dst_addr in self.routing_table:
                next_port = self.routing_table[packet.dst_addr][1]
                if next_port is not None:
                    self.send(next_port, packet) 
        else:
            # Gói tin định tuyến ROUTING: Hàng xóm gửi vector khoảng cách của họ
            neighbor_addr = packet.src_addr
            if neighbor_addr not in self.neighbors:
                return
            
            vector = json.loads(packet.content)
            # Cập nhật thông tin vector của lân cận đó vào bộ nhớ
            self.neighbors_vector[neighbor_addr] = vector
            
            # Tính toán lại đường đi xem có đường nào tốt hơn không (Thuật toán Bellman-Ford)
            updated = self.recompute_route()

            # Nếu đường đi thay đổi thì lập tức thông báo vector mới của mình cho tất cả lân cận
            if updated:
                self.broadcast_distance_vector()
            

    def handle_new_link(self, port, endpoint, cost):
        self.neighbors[endpoint] = (cost, port)
        self.neighbors_vector.setdefault(endpoint, {})  
        updated = self.recompute_route()
        if updated:
            self.broadcast_distance_vector()
        

    def handle_remove_link(self, port):
        remove_neighbor = None
        for neighbor, (_, neighbor_port) in self.neighbors.items():
            if neighbor_port == port:
                remove_neighbor = neighbor
                break
        
        if remove_neighbor:
            del self.neighbors[remove_neighbor]
            self.neighbors_vector.pop(remove_neighbor, None)

        updated = self.recompute_route()
        if updated:
            self.broadcast_distance_vector()
        

    def handle_time(self, time_ms):
        if time_ms - self.last_time >= self.heartbeat_time:
            self.last_time = time_ms

            self.broadcast_distance_vector()

    def recompute_route(self):
        """
        Thuật toán Bellman-Ford: Tính lại khoảng cách cực tiểu tới mọi node đích trong mạng.
        Công thức cơ bản: Dx(y) = min_v { c(x,v) + Dv(y) }
        Trả về True nếu bảng định tuyến có sự thay đổi.
        """
        # Luôn biết đường về bản thân với chi phí 0
        new_table = {self.addr : (0, None)}

        for nbr, (cost, port) in self.neighbors.items():
            new_table[nbr] = (cost, port)

        for neighbor_addr, vector in self.neighbors_vector.items():
            if neighbor_addr not in self.neighbors:
                continue
            neighbor_cost, neighbor_port = self.neighbors[neighbor_addr]
            for dest, cost in vector.items():
                if dest == self.addr:
                    continue
                total = neighbor_cost + cost
                if dest not in new_table or total < new_table[dest][0]:
                    new_table[dest] = (total, neighbor_port)
                        
            
        if new_table != self.routing_table:
            self.routing_table = new_table
            return True
        return False
    
    def broadcast_distance_vector(self):
        for port in self.links.keys():
            # Split Horizon: Không quảng bá đường đi cho lân cận nếu port chuyển tiếp là chính lân cận đó
            vector = {}
            for dest, (cost, next_port) in self.routing_table.items():
                if next_port != port:
                    vector[dest] = cost
                    
            packet = Packet(
                kind=Packet.ROUTING,
                src_addr=self.addr,
                dst_addr=None,
                content=json.dumps(vector)
            )
            self.send(port, packet)

    def __repr__(self):
        return f"DVrouter(addr={self.addr}, table={self.routing_table}, neighbors={self.neighbors})"