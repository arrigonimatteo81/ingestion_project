from processor.update_strategy.registro_update_strategy import RegistroUpdateStrategy, IdAndDateUpdateStrategy, \
    OnlyIdUpdateStrategy, NoOpRegistroUpdateStrategy, FileUpdateStrategy


class RegistroUpdateStrategyFactory:

    @staticmethod
    def create(tipo_caricamento: str) -> RegistroUpdateStrategy:
        if tipo_caricamento.upper() in ("CLIENTI", "RAPPORTI"):
            return IdAndDateUpdateStrategy()
        if tipo_caricamento.upper()in ("DATI", "DOMINI", "TABUT", "ESTRAZ"):
            return OnlyIdUpdateStrategy()
        if tipo_caricamento.upper()in ("DATI_RETT","CLIENTI_RETT", "RAPPORTI_RETT"):
            return FileUpdateStrategy()
        return NoOpRegistroUpdateStrategy()