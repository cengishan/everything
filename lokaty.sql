--ZADANIE 1, zakładam, że dostałem próbki danych, a w całym zbiorze ID_WALUTY_BAZOWEJ może być inne niż 1. Skrypt przeliczy mnożnik do waluty pośredniej, a następnie do PLN. Można jeszcze rozważyć stworzenie tabeli tymczasowej dla walut, ale na chwilę obecną wole nie zaśmiecać bazy.
with LOKATY_BAZOWE as (
	select 
	lok.*,
	lok.KWOTA * wal.PRZEL_DO_PLN as KWOTA_BAZOWA
	FROM
	LOKATY lok
	JOIN (select 
			wal.*,
			PRZELICZNIK_DO_WALUTY_BAZOWEJ as PRZEL_DO_PLN
		from 
		WALUTY wal
		where ID_WALUTY in (select ID_WALUTY from WALUTY where SYMBOL_WALUTY ='PLN')
		union all
		select 
		wal.*,
		--substr(replace( sys_connect_by_path(PRZELICZNIK_DO_WALUTY_BAZOWEJ, '*'),',','.'),2 ) as OBLICZENIA
		XMLQuery(substr(replace(sys_connect_by_path(PRZELICZNIK_DO_WALUTY_BAZOWEJ*10000, '*'),',','.'),2 ) RETURNING CONTENT).getnumberval() / power(10000,level) AS PRZEL_DO_PLN -- XMLQuery mial problem z ulamkami, wiec wymnozono do calkowitych
		from 
		(select * 
		from WALUTY
		where  ID_WALUTY not in (select ID_WALUTY from WALUTY where SYMBOL_WALUTY ='PLN')) wal
		START WITH ID_WALUTY_BAZOWEJ = 1 
		CONNECT BY PRIOR  ID_WALUTY =ID_WALUTY_BAZOWEJ) wal ON (lok.ID_WALUTY=wal.ID_WALUTY))
select 
cast(round(SUM(lok2.KWOTA_BAZOWA*lok2.OPROC)/SUM(lok2.KWOTA)*100,3) as VARCHAR(6))||'%' as SREDNIE_OPROCENTOWANIE
FROM
LOKATY_BAZOWE lok2
WHERE ID_KLIENT=15;


--ZADANIE 2
/*Jako klucz glówny wybrałbym ID_PRZELEWU. Jednoznacznie identyfikuje rekord w bazie danych. Pola ID_ODBIORCY lub ID_NADWCY mogą być nieunikalne. Pola tj jak data czy kwota nie są branę w ogólę pod uwagę ze względy na to, że nie są i nie mogą być kluczami.*/

--ZADANIE 3
/*Zapytanie zwróci 85. Wszsytkich rekordów jest 100, 15 rekordów ma wartość pola KOL = 5, pozostałe rekordy mają wartość pola różną od 5. 
100 - 15 = 85*/

--ZADANIE 4, zakładam, że przelewów może być więcej niż jeden w danym miesiącu, dlatego skrypt liczy miesięczne wynagrodzenie na następnie oblicza średnia wszystkich pracowników w całej firmie (a nie dla każdego pracownika)
with WYNAGRO_MIES as (
	select 
	prac.ID_PRACOWNIKA,
	prac.IMIE,
	extract(year from przel.DATA) as ROK,
	extract(month from przel.DATA) as MIESIAC,
	sum(przel.KWOTA * wal.PRZELICZNIK_DO_WALUTY_BAZOWEJ) as WYNAGRODZENIE_MIES
	FROM
	PRZELEWY_WYNAGRODZENIA przel
	JOIN (select 
			wal.*,
			PRZELICZNIK_DO_WALUTY_BAZOWEJ as PRZEL_DO_PLN
		from 
		WALUTY wal
		where ID_WALUTY in (select ID_WALUTY from WALUTY where SYMBOL_WALUTY ='PLN')
		union all
		select 
		wal.*,
		--substr(replace( sys_connect_by_path(PRZELICZNIK_DO_WALUTY_BAZOWEJ, '*'),',','.'),2 ) as OBLICZENIA
		XMLQuery(substr(replace(sys_connect_by_path(PRZELICZNIK_DO_WALUTY_BAZOWEJ*10000, '*'),',','.'),2 ) RETURNING CONTENT).getnumberval() / power(10000,level) AS PRZEL_DO_PLN -- XMLQuery mial problem z ulamkami, wiec wymnozono do calkowitych
		from 
		(select * 
		from WALUTY
		where  ID_WALUTY not in (select ID_WALUTY from WALUTY where SYMBOL_WALUTY ='PLN')) wal
		START WITH ID_WALUTY_BAZOWEJ = 1 
		CONNECT BY PRIOR  ID_WALUTY =ID_WALUTY_BAZOWEJ) wal ON (przel.ID_WALUTY=wal.ID_WALUTY)
	JOIN PRACOWNICY prac on (prac.ID_PRACOWNIKA=przel.ID_PRACOWNIKA) 
    group by prac.ID_PRACOWNIKA, prac.IMIE, extract(year from przel.DATA), extract(month from przel.DATA))
select 
avg(WYNAGRODZENIE_MIES) as SREDNIE_WYNAGRODZENIE
from
WYNAGRO_MIES;
