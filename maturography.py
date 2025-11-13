import math

class MaturographyCalculator:
    def __init__(self, age, lustrum_totals):
        """
        age: int
        lustrum_totals: list of totals for L1-L24 (length 24)
        """
        self.age = int(age)
        self.lustrum_totals = lustrum_totals

    def calculate(self):
        age = self.age
        totals = self.lustrum_totals

        # ---------- OBSERVED HUMAN MATUROGRAPHY ----------
        max_score = max(totals)
        a_ohm = len(totals) - totals[::-1].index(max_score)
        b_ohm = math.ceil(a_ohm / 2)

        if a_ohm <= 6:
            c_ohm = 1
        elif a_ohm <= 12:
            c_ohm = 2
        elif a_ohm <= 18:
            c_ohm = 3
        else:
            c_ohm = 4

        if a_ohm <= 3:
            d_ohm = 1
        elif a_ohm <= 6:
            d_ohm = 2
        else:
            d_ohm = 3

        ohm = (age - 1) + (a_ohm * b_ohm * c_ohm * d_ohm)

        # ---------- PREDICTED HUMAN MATUROGRAPHY ----------
        a_phm = math.ceil(age / 5)
        b_phm = math.ceil(age / 10)
        c_phm = 1 if age <= 30 else 2 if age <= 60 else (3 if age <= 90 else 4)
        d_phm = 1 if age <= 15 else (2 if age <= 30 else 3)

        phm = (age - 1) + (a_phm * b_phm * c_phm * d_phm)
        percentage_hm = round((ohm / phm) * 100, 2)

        # ---------- MATURITY ZONE ----------
        if 1 <= percentage_hm <= 49:
            zone = "Red Zone (Formative Maturity)"
        elif 50 <= percentage_hm <= 84:
            zone = "Yellow Zone (Functional Maturity)"
        elif 85 <= percentage_hm <= 94:
            zone = "Green Zone (Fulfilled Maturity)"
        elif 95 <= percentage_hm <= 100:
            zone = "Blue Zone (Transcendent Maturity)"
        elif percentage_hm > 100:
            zone = "Violet Zone (Cosmetic Maturity)"
        else:
            zone = "Red Zone (Formative Maturity)"

        return {
            "Observed": {
                "a_ohm": a_ohm,
                "b_ohm": b_ohm,
                "c_ohm": c_ohm,
                "d_ohm": d_ohm,
                "ohm": ohm
            },
            "Predicted": {
                "a_phm": a_phm,
                "b_phm": b_phm,
                "c_phm": c_phm,
                "d_phm": d_phm,
                "phm": phm
            },
            "percentage_hm": percentage_hm,
            "zone": zone
        }
