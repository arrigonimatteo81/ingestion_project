from processor.update_strategy.registro_update_strategy import RegistroUpdateStrategy, IdAndDateUpdateStrategy, \
    OnlyIdUpdateStrategy


class RegistroUpdateStrategyFactory:

    def create(self, tipo_caricamento: str) -> RegistroUpdateStrategy:
        if tipo_caricamento.upper() in ("CLIENTI", "RAPPORTI","CLIENTI_"):
            return IdAndDateUpdateStrategy()
        if tipo_caricamento.upper() in ("DATI", "DOMINI"):
            return OnlyIdUpdateStrategy()
        raise ValueError(tipo_caricamento)