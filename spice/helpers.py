from __future__ import annotations


def get_dune_network_name(network: str) -> str:
    if network == 'avalanche':
        return 'avalanche_c'
    elif network == 'bsc':
        return 'bnb'
    else:
        return network
