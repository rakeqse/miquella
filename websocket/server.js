import WebSocket, { WebSocketServer } from "ws";
const wss = new WebSocketServer({ port: 7731 })

wss.on('connection', function connection(ws) {
	ws.on('message', function message(data, isBinary) {
		wss.clients.forEach(function each(client) {
			if (client !== ws && client.readyState === WebSocket.OPEN) {
				client.send(data, {binary:isBinary});
			}
		});
	});
  ws.on('error', err=>console.log(err))
});
