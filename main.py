import socket
import threading
import os
import sys
import base64
import time
import statistics

estatisticas = {} # chave = (chunk_size, n_peers, tam_arquivo), valor = [tempos]

peers = {}  # chave: "ip:porta", valor: (status, relogio)
clock = 0
arquivos_recebidos = {}  # peer -> lista de arquivos "nome:tamanho"

tamanho_chunk = 256

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
    remetente, clock_msg, tipo = partes[:3]
    print(f"Mensagem recebida: \"{msg.strip()}\"")
    atualizar_relogio(clock_msg)
    clock_remetente = int(clock_msg)

    if remetente not in peers:
        peers[remetente] = ("ONLINE", clock_remetente)
        print(f"Adicionando novo peer {remetente} status ONLINE")
    else:
        status_atual, relogio_atual = peers[remetente]
        if tipo == "BYE":
            peers[remetente] = ("OFFLINE", max(clock_remetente, relogio_atual))
            print(f"Atualizando peer {remetente} status OFFLINE")
        else:
            if clock_remetente > relogio_atual:
                peers[remetente] = ("ONLINE", clock_remetente)
                print(f"Atualizando peer {remetente} status ONLINE")

    if tipo == "GET_PEERS":
        enviar_peer_list(remetente)
    elif tipo == "PEER_LIST":
        qtd = int(partes[3])
        for i in range(qtd):
            info = partes[4 + i].split(":")
            addr = f"{info[0]}:{info[1]}"
            status = info[2]
            relogio_recebido = int(info[3])
            if addr == identidade:
                continue
            if addr not in peers:
                peers[addr] = (status, relogio_recebido)
                print(f"Adicionando novo peer {addr} status {status}")
            else:
                status_atual, relogio_atual = peers[addr]
                if relogio_recebido > relogio_atual:
                    peers[addr] = (status, relogio_recebido)
                    print(f"Atualizando peer {addr} status {status}")
    elif tipo == "LS":
        arquivos = os.listdir(diretorio)
        arquivos_info = [f"{nome}:{os.path.getsize(os.path.join(diretorio, nome))}" for nome in arquivos]
        atualizar_relogio()
        msg = f"{identidade} {clock} LS_LIST {len(arquivos_info)} {' '.join(arquivos_info)}\n"
        enviar_mensagem(msg, remetente)
    elif tipo == "LS_LIST":
        qtd = int(partes[3])
        arquivos_recebidos[remetente] = partes[4:4+qtd]
    elif tipo == "DL":
        nome = partes[3]
        chunk_size = int(partes[4])
        chunk_index = int(partes[5])
        try:
            with open(os.path.join(diretorio, nome), "rb") as f:
                f.seek(chunk_index * chunk_size)
                dados = f.read(chunk_size)
                conteudo = base64.b64encode(dados).decode()
            atualizar_relogio()
            msg = f"{identidade} {clock} FILE {nome} {chunk_size} {chunk_index} {conteudo}\n"
            enviar_mensagem(msg, remetente)
        except:
            print(f"Erro ao ler chunk {chunk_index} do arquivo {nome}")
    elif tipo == "FILE":
        nome = partes[3]
        chunk_size = int(partes[4])
        chunk_index = int(partes[5])
        conteudo = " ".join(partes[6:])
        if conteudo == "null":
            dados = b""
        else:
            dados = base64.b64decode(conteudo)

        if nome not in arquivos_recebidos:
            arquivos_recebidos[nome] = {}
        arquivos_recebidos[nome][chunk_index] = dados

        chunks_recebidos = arquivos_recebidos[nome]
        total_chunks = max(chunks_recebidos.keys()) + 1
        if len(chunks_recebidos) == total_chunks:
            with open(os.path.join(diretorio, nome), "wb") as f:
                for i in range(total_chunks):
                    f.write(chunks_recebidos[i])
            print(f"Download do arquivo {nome} finalizado.")


def enviar_peer_list(destino):
    lista_peers = [
        f"{addr.split(':')[0]}:{addr.split(':')[1]}:{status}:{relogio}"
        for addr, (status, relogio) in peers.items() if addr != destino
    ]
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
        msg = conn.recv(1024 * 64).decode()
        tratar_mensagem(msg, identidade)


def listar_peers():
    print("Lista de peers:")
    print("[0] voltar para o menu anterior")
    for i, (addr, (status, _)) in enumerate(peers.items(), 1):
        print(f"[{i}] {addr} {status}")
    escolha = input("> ")
    if escolha == "0":
        return
    try:
        idx = int(escolha) - 1
        destino = list(peers.keys())[idx]
        atualizar_relogio()
        msg = f"{identidade} {clock} HELLO\n"
        if enviar_mensagem(msg, destino):
            peers[destino] = ("ONLINE", clock)
        else:
            peers[destino] = ("OFFLINE", clock)
        print(f"Atualizando peer {destino} status {peers[destino][0]}")
    except:
        print("Escolha inválida")


def obter_peers():
    for addr in peers:
        atualizar_relogio()
        msg = f"{identidade} {clock} GET_PEERS\n"
        if enviar_mensagem(msg, addr):
            peers[addr] = ("ONLINE", clock)
        else:
            peers[addr] = ("OFFLINE", clock)
        print(f"Atualizando peer {addr} status {peers[addr][0]}")


def listar_arquivos():
    arquivos = os.listdir(diretorio)
    for a in arquivos:
        print(a)


def buscar_arquivos():
    global arquivos_recebidos, estatisticas
    arquivos_recebidos = {}

    for peer, (status, _) in peers.items():
        if status == "ONLINE":
            atualizar_relogio()
            msg = f"{identidade} {clock} LS\n"
            enviar_mensagem(msg, peer)
    time.sleep(2)

    agrupados = {}  # chave: (nome, tamanho), valor: set(peers)
    for peer, lista in arquivos_recebidos.items():
        for entrada in lista:
            nome, tamanho = entrada.split(":")
            chave = (nome, tamanho)
            if chave not in agrupados:
                agrupados[chave] = set()
            agrupados[chave].add(peer)

    print("\nArquivos encontrados na rede:")
    print("Nome | Tamanho | Peer")
    print("[0] <Cancelar>")
    opcoes = []
    for i, ((nome, tamanho), peers_set) in enumerate(agrupados.items(), 1):
        print(f"[{i}] {nome} | {tamanho} | {', '.join(peers_set)}")
        opcoes.append((nome, int(tamanho), list(peers_set)))

    escolha = input("Digite o numero do arquivo para fazer o download:\n> ")
    if escolha == "0":
        return
    try:
        idx = int(escolha) - 1
        nome, tamanho, peers_disponiveis = opcoes[idx]
        print(f"arquivo escolhido {nome}")

        inicio = time.time()

        chunks = [None] * ((tamanho + tamanho_chunk - 1) // tamanho_chunk)
        threads = []

        def baixar_chunk(peer, chunk_index):
            global clock
            atualizar_relogio()
            msg = f"{identidade} {clock} DL {nome} {tamanho_chunk} {chunk_index}\n"
            enviar_mensagem(msg, peer)

        for i in range(len(chunks)):
            peer = peers_disponiveis[i % len(peers_disponiveis)]
            t = threading.Thread(target=baixar_chunk, args=(peer, i))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        time.sleep(2)

        if nome in arquivos_recebidos and isinstance(arquivos_recebidos[nome], dict):
            recebidos = arquivos_recebidos[nome]
            if len(recebidos) == len(chunks):
                with open(os.path.join(diretorio, nome), "wb") as f:
                    for i in range(len(chunks)):
                        f.write(recebidos[i])
                print(f"Download do arquivo {nome} finalizado.")

                fim = time.time()
                duracao = fim - inicio

                chave = (tamanho_chunk, len(peers_disponiveis), tamanho)
                if chave not in estatisticas:
                    estatisticas[chave] = []
                estatisticas[chave].append(duracao)
            else:
                print("Erro: nem todos os chunks foram recebidos.")
        else:
            print("Erro: chunks não recebidos corretamente.")
    except:
        print("Escolha inválida.")


def alterar_tamanho_chunk():
    global tamanho_chunk
    try:
        novo_valor = int(input("Digite novo tamanho de chunk:\n> "))
        if novo_valor <= 0:
            print("Valor inválido. Deve ser maior que 0.")
            return
        tamanho_chunk = novo_valor
        print(f"Tamanho de chunk alterado: {tamanho_chunk}")
    except ValueError:
        print("Entrada inválida.")


def exibir_estatisticas():
    print("\nTam. chunk | N peers | Tam. arquivo | N | Tempo [s] | Desvio")
    for (chunk, n_peers, tam), tempos in estatisticas.items():
        n = len(tempos)
        media = sum(tempos) / n
        desvio = statistics.stdev(tempos) if n > 1 else 0
        print(f"{chunk:^11} | {n_peers:^7} | {tam:^12} | {n:^3} | {media:.5f} | {desvio:.5f}")


def sair():
    for addr, (status, _) in peers.items():
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
        if peer and peer != identidade:
            peers[peer] = ("OFFLINE", 0)
            print(f"Adicionando novo peer {peer} status OFFLINE")

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
    cmd = input("> ")
    if cmd == "1":
        listar_peers()
    elif cmd == "2":
        obter_peers()
    elif cmd == "3":
        listar_arquivos()
    elif cmd == "4":
        buscar_arquivos()
    elif cmd == "5":
        exibir_estatisticas()
    elif cmd == "6":
        alterar_tamanho_chunk()
    elif cmd == "9":
        sair()
    else:
        print("Comando não implementado nesta parte.")
