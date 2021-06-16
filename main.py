from clic.clic_abiertos import clic_abiertos
from clic.clic_resueltos import clic_resueltos
from clic.poblar_dims_clic import dimensiones_clic_abiertos, dimensiones_clic_resueltos
# from clic.fact_clic import fact_clic_abiertos, fact_clic_resueltos
# from avaya.llamadas import av_llamadas
# from avaya.abandonos import av_abandonos
# from avaya.poblar_dims_avaya import dims_avaya_llamadas, dims_avaya_abandonos
# from avaya.fact_avaya import fact_av_llamadas, fact_av_abandonos
from service_manager.sm_backlog import sm_backlog
from service_manager.sm_cerrado import sm_cerrado
from service_manager.poblar_dims_sm import dimensiones_sm_backlog, dimensiones_sm_cerrado
from service_manager.fact_sm import fact_sm_backlog, fact_sm_cerrado
import time
import datetime


# def clic():
#     clic_abiertos()
#     clic_resueltos()
#     dimensiones_clic_abiertos()
#     dimensiones_clic_resueltos()
#     fact_clic_abiertos()
#     fact_clic_resueltos()


# def avaya():
#     av_llamadas()
#     av_abandonos()
#     dims_avaya_llamadas()
#     dims_avaya_abandonos()
#     fact_av_llamadas()
#     fact_av_abandonos()


def services_manager():
    sm_cerrado()
    sm_backlog()
    dimensiones_sm_cerrado()
    dimensiones_sm_backlog()
    fact_sm_cerrado()
    fact_sm_backlog()


if __name__ == '__main__':
    start = time.time()
    print(f"Fecha de Inicio: {datetime.datetime.now()}")
    clic()
    avaya()
    services_manager()
    end = time.time()
    print(end - start)
    print(f"Fecha Finalización: {datetime.datetime.now()}")
