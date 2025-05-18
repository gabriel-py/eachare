import socket
import threading
import os
import sys


peers = {}
clock = 0


def atualizar_relogio(clock_msg=None):
    global clock
    if clock_msg is not None:
        clock = max(clock, int(clock_msg))
    clock += 1
    print(f"=> Atualizando relogio para {clock}")
    return clock


def enviar_mensagem(msg, destino):
    try:
        ip, porta = destino.split(":")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((ip, int(porta)))
            s.sendall(msg.encode())
        print(f"Encaminhando mensagem \"{msg.strip()}\" para {destino}")
        return True
    except:
        return False

def tratar_mensagem(msg, origem):
    global peers
    partes = msg.strip().split()
    if len(partes) < 3:
        return
    remetente, _, tipo = partes[:3]
    print(f"Mensagem recebida: \"{msg.strip()}\"")
    atualizar_relogio()

    if tipo == "HELLO":
        peers[remetente] = "ONLINE"
        print(f"Atualizando peer {remetente} status ONLINE")
    elif tipo == "BYE":
        peers[remetente] = "OFFLINE"
        print(f"Atualizando peer {remetente} status OFFLINE")
    elif tipo == "GET_PEERS":
        peers[remetente] = "ONLINE"
        enviar_peer_list(remetente)
    elif tipo == "PEER_LIST":
        qtd = int(partes[3])
        for i in range(qtd):
            info = partes[4 + i].split(":")
            addr = f"{info[0]}:{info[1]}"
            status = info[2]
            if addr != origem:
                if addr not in peers:
                    peers[addr] = status
                    print(f"Adicionando novo peer {addr} status {status}")
                else:
                    peers[addr] = status
                    print(f"Atualizando peer {addr} status {status}")

def enviar_peer_list(destino):
    lista_peers = [f"{addr}:{status}:0" for addr, status in peers.items() if addr != destino]
    msg = f"{identidade} {clock} PEER_LIST {len(lista_peers)} {' '.join(lista_peers)}\n"
    enviar_mensagem(msg, destino)


def servidor_tcp():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind((host, int(porta)))
    srv.listen()
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=tratar_conexao, args=(conn,)).start()

def tratar_conexao(conn):
    with conn:
        msg = conn.recv(1024).decode()
        tratar_mensagem(msg, identidade)


def listar_peers():
    print("Lista de peers:")
    print("[0] voltar para o menu anterior")
    for i, (addr, status) in enumerate(peers.items(), 1):
        print(f"[{i}] {addr} {status}")
    escolha = input(">")
    if escolha == "0":
        return
    try:
        idx = int(escolha) - 1
        destino = list(peers.keys())[idx]
        atualizar_relogio()
        msg = f"{identidade} {clock} HELLO\n"
        if enviar_mensagem(msg, destino):
            peers[destino] = "ONLINE"
        else:
            peers[destino] = "OFFLINE"
        print(f"Atualizando peer {destino} status {peers[destino]}")
    except:
        print("Escolha inválida")

def obter_peers():
    for addr in peers:
        atualizar_relogio()
        msg = f"{identidade} {clock} GET_PEERS\n"
        if enviar_mensagem(msg, addr):
            peers[addr] = "ONLINE"
        else:
            peers[addr] = "OFFLINE"
        print(f"Atualizando peer {addr} status {peers[addr]}")

def listar_arquivos():
    arquivos = os.listdir(diretorio)
    for a in arquivos:
        print(a)

def sair():
    for addr, status in peers.items():
        if status == "ONLINE":
            atualizar_relogio()
            msg = f"{identidade} {clock} BYE\n"
            enviar_mensagem(msg, addr)
    print("Saindo...")
    os._exit(0)


if len(sys.argv) != 4:
    print("Uso: python main.py <endereco:porta> <vizinhos.txt> <diretorio_compartilhado>")
    sys.exit(1)

identidade = sys.argv[1]
host, porta = identidade.split(":")
arquivo_peers = sys.argv[2]
diretorio = sys.argv[3]

if not os.path.isdir(diretorio):
    print("Diretório de compartilhamento inválido.")
    sys.exit(1)

with open(arquivo_peers) as f:
    for linha in f:
        peer = linha.strip()
        if peer:
            if peer == identidade:
                continue
            status = "OFFLINE"
            peers[peer] = status
            print(f"Adicionando novo peer {peer} status {status}")

threading.Thread(target=servidor_tcp, daemon=True).start()

while True:
    print("""
Escolha um comando:
[1] Listar peers
[2] Obter peers
[3] Listar arquivos locais
[4] Buscar arquivos
[5] Exibir estatisticas
[6] Alterar tamanho de chunk
[9] Sair
""")
    cmd = input(">")
    if cmd == "1":
        listar_peers()
    elif cmd == "2":
        obter_peers()
    elif cmd == "3":
        listar_arquivos()
    elif cmd == "9":
        sair()
    else:
        print("Comando não implementado nesta parte.")
