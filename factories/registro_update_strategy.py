from processor.update_strategy.registro_update_strategy import RegistroUpdateStrategy, IdAndDateUpdateStrategy, \
    OnlyIdUpdateStrategy


class RegistroUpdateStrategyFactory:

    def create(self, tipo_caricamento: str) -> RegistroUpdateStrategy:
        if tipo_caricamento in ("CLIENTI", "RAPPORTI"):
            return IdAndDateUpdateStrategy()
        if tipo_caricamento in ("DATI", "DOMINI"):
            return OnlyIdUpdateStrategy()
        raise ValueError(tipo_caricamento)