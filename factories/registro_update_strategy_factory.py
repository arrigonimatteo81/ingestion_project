from processor.update_strategy.registro_update_strategy import RegistroUpdateStrategy, IdAndDateUpdateStrategy, \
    OnlyIdUpdateStrategy, NoOpRegistroUpdateStrategy


class RegistroUpdateStrategyFactory:

    @staticmethod
    def create(tipo_caricamento: str) -> RegistroUpdateStrategy:
        if tipo_caricamento.upper().startswith(("CLIENTI", "RAPPORTI")):
            return IdAndDateUpdateStrategy()
        if tipo_caricamento.upper().startswith(("DATI", "DOMINI")):
            return OnlyIdUpdateStrategy()
        return NoOpRegistroUpdateStrategy()