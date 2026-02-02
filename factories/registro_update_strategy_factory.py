from processor.update_strategy.registro_update_strategy import RegistroUpdateStrategy, IdAndDateUpdateStrategy, \
    OnlyIdUpdateStrategy, NoOpRegistroUpdateStrategy


class RegistroUpdateStrategyFactory:

    @staticmethod
    def create(tipo_caricamento: str) -> RegistroUpdateStrategy:
        if tipo_caricamento.upper() in ("CLIENTI", "RAPPORTI"):
            return IdAndDateUpdateStrategy()
        if tipo_caricamento.upper()in ("DATI", "DOMINI", "TABUT", "ESTRAZ"):
            return OnlyIdUpdateStrategy()
        return NoOpRegistroUpdateStrategy()