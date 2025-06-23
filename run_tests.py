#!/usr/bin/env python3
"""
Script para executar todos os testes do projeto afonsystem
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Executa um comando e exibe o resultado"""
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("⚠️  STDERR:")
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"✅ {description} - SUCESSO")
            return True
        else:
            print(f"❌ {description} - FALHOU (código: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar comando: {e}")
        return False


def main():
    """Função principal"""
    print("🧪 SUITE DE TESTES - AFONSYSTEM")
    print("=" * 60)
    
    # Verificar se pytest está instalado
    print("🔍 Verificando dependências...")
    try:
        import pytest
        print("✅ pytest encontrado")
    except ImportError:
        print("❌ pytest não encontrado. Instalando...")
        if not run_command("pip install pytest pytest-mock pytest-cov", "Instalando pytest"):
            print("❌ Falha ao instalar pytest")
            return 1
    
    # Verificar se as outras dependências estão instaladas
    try:
        import pandas
        import pydantic
        print("✅ Dependências principais encontradas")
    except ImportError as e:
        print(f"❌ Dependência faltando: {e}")
        print("💡 Execute: pip install -r requirements.txt")
        return 1
    
    success_count = 0
    total_tests = 6
    
    # Lista de comandos de teste
    test_commands = [
        ("pytest tests/test_data_collector.py -v", "Testes do GitHubDataCollector"),
        ("pytest tests/test_snapshot_manager.py -v", "Testes do SnapshotManager"),
        ("pytest tests/test_supabase_helper.py -v", "Testes do SupabaseHelper"),
        ("pytest tests/test_models.py -v", "Testes dos Models"),
        ("pytest tests/ -v --tb=short", "Executando todos os testes"),
        ("pytest tests/ --cov=helpers --cov=models --cov-report=term-missing", "Relatório de cobertura")
    ]
    
    # Executar cada comando de teste
    for command, description in test_commands:
        if run_command(command, description):
            success_count += 1
    
    # Relatório final
    print(f"\n{'='*60}")
    print("📊 RELATÓRIO FINAL")
    print(f"{'='*60}")
    print(f"✅ Testes bem-sucedidos: {success_count}/{total_tests}")
    print(f"❌ Testes com falha: {total_tests - success_count}/{total_tests}")
    
    if success_count == total_tests:
        print("🎉 Todos os testes passaram!")
        return 0
    else:
        print("⚠️  Alguns testes falharam. Verifique os logs acima.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)