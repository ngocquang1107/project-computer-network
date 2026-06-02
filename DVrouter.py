import json
from router import Router
from packet import Packet


class DVrouter(Router):
    def __init__(self, addr, heartbeat_time):
        Router.__init__(self, addr)
        self.heartbeat_time = heartbeat_time
        self.last_time = 0

        self.neighbors = {}
        self.neighbors_vector = {}
        self.routing_table = {addr : (0, None)}

    def handle_packet(self, port, packet : Packet):
        """Process incoming packet."""

        if packet.is_traceroute:
            if packet.dst_addr in self.routing_table:
                next_port = self.routing_table[packet.dst_addr][1]
                if next_port is not None:
                    self.send(next_port, packet) 
        else:
            neighbor_addr = packet.src_addr
            vector = json.loads(packet.content)
            self.neighbors_vector[neighbor_addr] = vector
            updated = self.recompute_route()

            if updated:
                self.broadcast_distance_vector()
            

    def handle_new_link(self, port, endpoint, cost):
        """Handle new link."""

        self.neighbors[endpoint] = (cost, port)
        self.neighbors_vector.setdefault(endpoint, {})  
        updated = self.recompute_route()
        if updated:
            self.broadcast_distance_vector()
        

    def handle_remove_link(self, port):
        """Handle removed link."""

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
        """Handle current time."""
        if time_ms - self.last_time >= self.heartbeat_time:
            self.last_time = time_ms

            self.broadcast_distance_vector()

    def recompute_route(self):
        new_table = {self.addr : (0, None)}

        for nbr, (cost, port) in self.neighbors.items():
            new_table[nbr] = (cost, port)

        for neighbor_addr, vector in self.neighbors_vector.items():
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
        vector = {dest : cost for dest, (cost, _) in self.routing_table.items()}
        packet = Packet(
            kind=Packet.ROUTING,
            src_addr=self.addr,
            dst_addr=None,
            content=json.dumps(vector)
        )

        for port in self.links.keys():
            self.send(port, packet)

    def __repr__(self):
        """Representation for debugging in the network visualizer."""    
        return f"DVrouter(addr={self.addr}, table={self.routing_table}, neighbors={self.neighbors})"