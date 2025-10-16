"""Main entry point for the VAMDC CLI."""

try:
    # Try relative import first
    from .cli import cli
except ImportError:
    # Fall back to absolute import
    from pyVAMDC.spectral.cli import cli

if __name__ == '__main__':
    cli()
