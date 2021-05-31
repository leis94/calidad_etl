from clic.clic_abiertos import clic_abiertos
from clic.clic_resueltos import clic_resueltos
from clic.poblar_dims_clic import dimensiones_clic_abiertos, dimensiones_clic_resueltos
from clic.fact_clic import fact_clic_abiertos, fact_clic_resueltos

if __name__ == '__main__':
    print("Inicio")
    clic_abiertos()
    clic_resueltos()
    dimensiones_clic_abiertos()
    dimensiones_clic_resueltos()
    fact_clic_abiertos()
    fact_clic_resueltos()
    print("Finalizó")