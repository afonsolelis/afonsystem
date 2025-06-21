#!/usr/bin/env python3
"""
Coletador de dados inicial - Script para coleta em lote de todos os repositórios
Este script será usado apenas uma vez para coleta inicial e depois deletado.
"""

import os
import time
from datetime import datetime
from helpers.data_collector import GitHubDataCollector
from dotenv import load_dotenv

def main():
    print("🚀 Iniciando coleta em lote de todos os repositórios...")
    print("=" * 60)
    
    # Load environment variables
    load_dotenv()
    
    # Initialize collector
    collector = GitHubDataCollector()
    
    # Get all repositories
    repos = collector.get_available_repos()
    
    if not repos:
        print("❌ Nenhum repositório configurado no .env")
        return
    
    print(f"📋 Encontrados {len(repos)} repositórios para coleta:")
    for i, repo in enumerate(repos, 1):
        print(f"   {i:2d}. {repo}")
    
    print("\n" + "=" * 60)
    
    # Tracking variables
    total_repos = len(repos)
    successful = 0
    failed = 0
    failed_repos = []
    
    start_time = time.time()
    
    # Process each repository
    for i, repo in enumerate(repos, 1):
        print(f"\n📦 [{i}/{total_repos}] Processando: {repo}")
        print(f"⏱️  Tempo decorrido: {time.time() - start_time:.1f}s")
        
        try:
            # Collect data for repository
            repo_start = time.time()
            db_path = collector.create_timestamped_db(repo)
            repo_duration = time.time() - repo_start
            
            if db_path:
                print(f"✅ Sucesso em {repo_duration:.1f}s - Arquivo: {db_path}")
                successful += 1
            else:
                print(f"❌ Falhou após {repo_duration:.1f}s")
                failed += 1
                failed_repos.append(repo)
                
        except Exception as e:
            print(f"❌ Erro durante coleta: {e}")
            failed += 1
            failed_repos.append(repo)
        
        # Progress bar
        progress = (i / total_repos) * 100
        filled = int(progress // 2)
        bar = "█" * filled + "░" * (50 - filled)
        print(f"📊 Progresso: [{bar}] {progress:.1f}%")
        
        # Add small delay to avoid rate limiting
        if i < total_repos:
            print("⏳ Aguardando 2s para evitar rate limit...")
            time.sleep(2)
    
    # Final summary
    total_time = time.time() - start_time
    print("\n" + "=" * 60)
    print("📊 RESUMO FINAL")
    print("=" * 60)
    print(f"⏱️  Tempo total: {total_time:.1f}s ({total_time/60:.1f} minutos)")
    print(f"✅ Sucessos: {successful}")
    print(f"❌ Falhas: {failed}")
    print(f"📈 Taxa de sucesso: {(successful/total_repos)*100:.1f}%")
    
    if failed_repos:
        print(f"\n❌ Repositórios que falharam:")
        for repo in failed_repos:
            print(f"   - {repo}")
        print(f"\n💡 Você pode tentar coletar estes repositórios individualmente via app.py")
    
    print(f"\n🎉 Coleta concluída! Dados salvos em: datalake/")
    print(f"🗑️  Você pode deletar este script agora: rm coletador.py")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Coleta interrompida pelo usuário")
        print("📊 Dados já coletados foram salvos em datalake/")
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        print("📊 Dados já coletados foram salvos em datalake/")