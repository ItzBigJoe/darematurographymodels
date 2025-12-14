from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, session
import sqlite3
import pandas as pd
from maturography import MaturographyCalculator
from datetime import timedelta

main = Flask(__name__)
main.secret_key = "your_super_secret_key"  # Change this to a secure random key
main.permanent_session_lifetime = timedelta(minutes=30)

# ------------------ DATABASE CONNECTION ------------------
import pg8000.dbapi
import os
from dotenv import load_dotenv

load_dotenv()




def get_db_connection():
    conn = pg8000.dbapi.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 5432)),
        ssl_context=True  # Enable SSL for Render PostgreSQL
    )
    return conn




def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS human_maturography_records (
            id SERIAL PRIMARY KEY,
            age INTEGER,
            marital_status TEXT,
            gender TEXT,
            occupation TEXT,
            education TEXT,
            fg_motor INTEGER, fg_language INTEGER, fg_interactions INTEGER, fg_emotional INTEGER,
            fg_curiosity INTEGER, fg_self_recognition INTEGER, fg_total INTEGER,
            sa_friendships INTEGER, sa_rules INTEGER, sa_empathy INTEGER, sa_self_regulation INTEGER,
            sa_independence INTEGER, sa_hobbies INTEGER, sa_total INTEGER,
            id_self_awareness INTEGER, id_peer_interest INTEGER, id_exploration INTEGER,
            id_emotional_challenges INTEGER, id_abstract_thinking INTEGER, id_physical_changes INTEGER, id_total INTEGER,
            ir_independence INTEGER, ir_responsibility INTEGER, ir_vocational INTEGER,
            ir_values INTEGER, ir_relationships INTEGER, ir_longterm_planning INTEGER, ir_total INTEGER,
            cr_transition_to_adult INTEGER, cr_focus_on_career INTEGER, cr_form_relationship INTEGER,
            cr_longterm_goals INTEGER, cr_manages_finances INTEGER, cr_explore_identity INTEGER, cr_total INTEGER,
            fa_establish_family INTEGER, fa_career_advancement INTEGER, fa_financial_planning INTEGER,
            fa_work_life_balance INTEGER, fa_build_home_env INTEGER, fa_support_networks INTEGER, fa_total INTEGER,
            st_stability_career INTEGER, st_self_refinement INTEGER, st_life_goal_adjustment INTEGER,
            st_health_focus INTEGER, st_work_life_balance INTEGER, st_community_contribution INTEGER, st_total INTEGER,
            ml_reflect_achievements INTEGER, ml_adjust_goals INTEGER, ml_focus_purpose INTEGER,
            ml_meaningful_activities INTEGER, ml_strengthen_relationships INTEGER, ml_address_challenges INTEGER, ml_total INTEGER,
            er_career_peak INTEGER, er_mentorship_roles INTEGER, er_children_focus INTEGER,
            er_legacy_investment INTEGER, er_community_service INTEGER, er_balance_responsibilities INTEGER, er_self_reflection INTEGER, er_total INTEGER,
            sa_nurtures_others INTEGER, sa_personal_goals INTEGER, sa_creative_interests INTEGER,
            sa_work_life_harmony INTEGER, sa_future_preparation INTEGER, sa_share_wisdom INTEGER, sa2_total INTEGER,
            ws_self_care INTEGER, ws_life_priority INTEGER, ws_mentoring_extensive INTEGER,
            ws_lifelong_learning INTEGER, ws_emotional_resilience INTEGER, ws_financial_planning INTEGER, ws_total INTEGER,
            pr_lifestyle_adjustment INTEGER, pr_meaningful_activities INTEGER, pr_health_management INTEGER,
            pr_legacy_building INTEGER, pr_career_transition INTEGER, pr_retirement INTEGER, pr_total INTEGER,
            lr_community_contribution INTEGER, lr_document_life INTEGER, lr_family_connections INTEGER,
            lr_reflect_achievements INTEGER, lr_social_engagement INTEGER, lr_emotional_stability INTEGER, lr_total INTEGER,
            em_mental_wellbeing INTEGER, em_gratitude INTEGER, em_life_acceptance INTEGER,
            em_simple_joy INTEGER, em_positive_outlook INTEGER, em_counsel_others INTEGER, em_total INTEGER,
            wm_offer_councel INTEGER, wm_find_peace INTEGER, wm_foster_resilience INTEGER,
            wm_shares_stories INTEGER, wm_spiritual_pursuits INTEGER, wm_meaningful_relationship INTEGER, wm_total INTEGER,
            ar_adapts_to_health INTEGER, ar_strengthen_relationships INTEGER, ar_pass_on_traditions INTEGER,
            ar_life_reflection INTEGER, ar_resilience_aging INTEGER, ar_maintain_independence INTEGER, ar_total INTEGER,
            sr_serenity_practices INTEGER, sr_life_reflection INTEGER, sr_family_milestones INTEGER,
            sr_quiet_pursuits INTEGER, sr_support_networks INTEGER, sr_sense_of_purpose INTEGER, sr_total INTEGER,
            lf_storytelling INTEGER, lf_inspire_future INTEGER, lf_spiritual_beliefs INTEGER,
            lf_family_connections INTEGER, lf_focus_legacy INTEGER, lf_accept_assistance INTEGER, lf_total INTEGER,
            co_inner_peace INTEGER, co_simplicity INTEGER, co_strengthen_bonds INTEGER,
            co_express_gratitude INTEGER, co_positive_memories INTEGER, co_prioritize_wellbeing INTEGER, co_total INTEGER,
            rs_final_reflection INTEGER, rs_preserve_memories INTEGER, rs_support_systems INTEGER,
            rs_spiritual_closure INTEGER, rs_share_wisdom INTEGER, rs_end_of_life INTEGER, rs_total INTEGER,
            ex_century_reflection INTEGER, ex_share_wisdom INTEGER, ex_family_unity INTEGER,
            ex_celebrate_centenarian INTEGER, ex_mental_engagement INTEGER, ex_historical_perspective INTEGER, ex_total INTEGER,
            pa_life_stages INTEGER, pa_dignity INTEGER, pa_meaningful_connections INTEGER,
            pa_daily_comfort INTEGER, pa_caregivers INTEGER, pa_memories_solace INTEGER, pa_total INTEGER,
            gl_celebrate_longevity INTEGER, gl_express_gratitude INTEGER, gl_foster_peace INTEGER,
            gl_appreciate_legacy INTEGER, gl_share_insights INTEGER, gl_grateful_mindset INTEGER, gl_total INTEGER,
            fm_accept_life_cycle INTEGER, fm_pass_wisdom INTEGER, fm_find_closure INTEGER,
            fm_lifetime_reflection INTEGER, fm_support_system INTEGER, fm_final_peace INTEGER, fm_total INTEGER,
            observed_lustrum REAL, observed_decade REAL, observed_generation REAL,
            observed_life_stage REAL, observed_human_maturogram REAL,
            predicted_lustrum REAL, predicted_decade REAL, predicted_generation REAL,
            predicted_life_stage REAL, predicted_human_maturogram REAL,
            percentage_hm REAL, maturity_zone TEXT
        );
    ''')

    conn.commit()
    # Ensure sociodemographic columns exist for older DBs
    cur.execute("ALTER TABLE human_maturography_records ADD COLUMN IF NOT EXISTS marital_status TEXT;")
    cur.execute("ALTER TABLE human_maturography_records ADD COLUMN IF NOT EXISTS gender TEXT;")
    cur.execute("ALTER TABLE human_maturography_records ADD COLUMN IF NOT EXISTS occupation TEXT;")
    cur.execute("ALTER TABLE human_maturography_records ADD COLUMN IF NOT EXISTS education TEXT;")

    conn.commit()
    cur.close()
    conn.close()


init_db()

# ------------------ GLOBAL DATA ------------------
focus_texts = [
    "Foundational Growth (Motor, Attachment & Language)",
    "Social Awareness (Friends, Empathy & Rules)",
    "Identity Formation (Identity, Emotion & Peer Interest)",
    "Independence and Responsibility (Independence, Responsibility & Beliefs)",
    "Career and Relationship Focus (Career, Relationships & Goals)",
    "Family and Career Advancement (Family, Career, Finance)",
    "Stability and Growth (Stability, Refinement & Health)",
    "Mid-Life Reflection (Reflection, Purpose & Adjustment)",
    "Expanded Responsibility (Responsibility, Mentorship & Legacy)",
    "Self-Actualization (Self-Actualization, Fulfillment)",
    "Wisdom Sharing (Wisdom, Guidance & Priorities)",
    "Preparation for Later Years (Retirement Preparation and Planning)",
    "Legacy and Reflection (Legacy, Family & Contribution)",
    "Emotional Balance (Emotional Balance, Gratitude)",
    "Wisdom and Mentorship (Wisdom, Peace & Mentorship)",
    "Acceptance and Resilience (Acceptance, Resilience and Tradition)",
    "Serenity and Reflection (Serenity, Reflection and Spirituality)",
    "Legacy Fulfillment (Storytelling, Legacy and Family)",
    "Contentment and Inner Peace (Contentment, Peace and Simplicity)",
    "Completion of Legacy (Closure, Reflection and Spirituality)",
    "Extended Reflection (Century Reflection & Unity)",
    "Peaceful Acceptance (Peaceful Acceptance & Dignity)",
    "Gratitude for Longevity (Longevity, Gratitude & Peace)",
    "Final Milestones (Final Wisdom & Closure)"
]

checklist_items = [
    "Demonstrates basic motor skills like crawling, walking, and grasping objects.",
    "Acquires language fundamentals, such as babbling, first words, and simple sentences.",
    "Engages in initial social interactions, like playing with caregivers or siblings.",
    "Forms emotional attachments, showing separation anxiety or preference for familiar people.",
    "Exhibits curiosity about the environment through exploration and sensory play.",
    "Begins to recognize self in mirrors and responds to own name.",
    "Forms friendships with peers and participates in group play. ",
    "Understands and follows basic rules in games, school, or home settings. ",
    "Shows basic empathy, such as comforting a crying friend.",
    "Demonstrates self-regulation, like controlling impulses or waiting turns.",
    "Exhibits growing independence, such as dressing self or completing simple chores. ",
    "Develops interests in hobbies or activities outside family influence. ",
    "Increases self-awareness, questioning personal identity and values. ",
    "Shows strong interest in peer groups and social acceptance. ",
    "Explores identity through changes in appearance, interests, or beliefs. ",
    "Faces emotional challenges like mood swings or conflicts with authority",
    "Develops abstract thinking and begins to form opinions on broader issues. ",
    "Experiences physical changes associated with puberty. ",
    "Pursues greater independence, such as driving, part-time jobs, or leaving home for education.",
    "Takes on early adult responsibilities like managing finances or schedules. ",
    "Explores vocational interests through education or internships. ",
    "Solidifies personal values and beliefs, often through debate or activism.",
    "Builds romantic relationships and navigates social complexities.",
    "Develops long-term planning skills for future goals.",
    "Transitions to full adult roles,such as independent living or full- time work.",
    "Focuses on career-building through entry-level jobs or further education.",
    "Forms deeper personalrelationships, including serious partnerships.",
    "Begins considering long-term goals like home ownership or family planning.",
    "Manages personal finances more comprehensively.",
    "Explores self-identity in professional and social contexts.", 
    "Establishes a family, such as marriage or having children.", 
    "Advances in career through promotions or skill development.", 
    "Engages in financial planning, like saving for future needs.", 
    "Balances multiple responsibilities between work, family, and personal life.", 
    "Builds a stable home environment.", 
    "Develops stronger support networks with friends and family.",
    "Seeks stability in career and personal relationships.", 
    "Focuses on self-refinement through education or personal development.", 
    "Reassesses life goals and makes adjustments as needed.", 
    "Prioritizes health and well-being, such as regular exercise or diet.", 
    "Manages work-life balance more effectively.", 
    "Contributes to community or professional networks.", 
    "Reflects on past achievements and life satisfaction.", 
    "Adjusts personal and professional goals based on experiences.", 
    "Increases focus on life purpose and societal impact.", 
    "Pursues meaningful activities or causes.", 
    "Strengthens family bonds and friendships.", 
    "Addresses mid-life challenges like career plateaus or health concerns.", 
    "Reaches career peak with leadership positions.", 
    "Takes on mentorship roles for younger colleagues or family.", 
    "Focuses on children's development and education.", 
    "Invests in future legacy through savings or philanthropy.", 
    "Engages actively in community service.", 
    "Balances expanded responsibilities with personal fulfillment.", 
    "Recognizes personal strengths and limitations through self-reflection.", 
    "Nurtures younger generations via guidance or support.", 
    "Achieves fulfillment in long-term personal goals.", 
    "Pursues creative or intellectual interests.", 
    "Maintains work-life harmony.", 
    "Prepares for future life transitions.", 
    "Broadens influence by sharing wisdom in professional or personal circles.", 
    "Continues self-care routines for physical and mental health.", 
    "Re-evaluates life priorities and makes shifts.", 
    "Mentors others extensively.", 
    "Engages in lifelong learning.", 
    "Strengthens emotional resilience.", 
    "Plans financially for retirement, such as investments or pensions.", 
    "Adjusts lifestyle for future needs.", 
    "Pursues meaningful activities like hobbies or travel.", 
    "Manages health proactively with check-ups and lifestyle changes.", 
    "Builds legacy through family or community contributions.", 
    "Transitions career towards reduced hours.",
    "Enters retirement or reduces workload significantly.", 
    "Contributes via community service or volunteering.", 
    "Writes memoirs or documents life experiences.", 
    "Enjoys deeper family connections and gatherings.", 
    "Reflects on life achievements.", 
    "Maintains social engagements.", 
    "Focuses on emotional stability through practices like meditation.", 
    "Prioritizes mental well-being and seeks support if needed.", 
    "Fosters gratitude for past and present experiences.", 
    "Accepts life's stages with grace.", 
    "Finds joy in simpler aspects like nature or daily routines.", 
    "Maintains positive outlook despite changes.", 
    "Offers counsel and mentorship to younger family or community members.", 
    "Finds peace with life's accomplishments.", 
    "Fosters personal resilience and contentment.", 
    "Shares stories and lessons learned.", 
    "Engages in spiritual or philosophical pursuits.", 
    "Prioritizes meaningful relationships.", 
    "Adapts to health changes with aids or modifications.", 
    "Strengthens key relationships with family and friends.", 
    "Passes on traditions and cultural knowledge.", 
    "Appreciates life's journey through reflection.", 
    "Builds resilience against aging challenges.", 
    "Maintains independence where possible.",
    "Seeks serenity through mental and spiritual practices.", 
    "Reflects deeply on life experiences.", 
    "Celebrates family milestones and achievements.", 
    "Enjoys quiet pursuits like reading or contemplation.", 
    "Relies on support networks for daily needs.", 
    "Maintains a sense of purpose.", 
    "Engages in storytelling to preserve history.", 
    "Inspires future generations with insights.", 
    "Relies on spiritual beliefs for comfort.", 
    "Deepens family connections through interactions.", 
    "Focuses on legacy fulfillment.", 
    "Accepts assistance gracefully.", 
    "Emphasizes inner peace through mindfulness.", 
    "Embraces simplicity in daily life.", 
    "Strengthens bonds with loved ones.", 
    "Expresses gratitude for life's journey.", 
    "Reflects on positive memories.", 
    "Prioritizes comfort and well-being.", 
    "Engages in final reflections on life.", 
    "Preserves memories through photos or writings.", 
    "Relies on support systems for care.", 
    "Seeks spiritual closure and peace.", 
    "Shares final wisdom with others.", 
    "Accepts end-of-life transitions.", 
    "Reflects on a century's worth of experiences.", 
    "Shares profound life wisdom.", 
    "Fosters family unity and resilience.", 
    "Celebrates centenarian status.", 
    "Maintains mental engagement.", 
    "Appreciates historical perspectives.",
    "Embraces life's final stages peacefully.", 
    "Focuses on maintaining dignity.", 
    "Prioritizes meaningful connections.", 
    "Seeks comfort in daily routines.", 
    "Relies heavily on caregivers.", 
    "Finds solace in memories.", 
    "Celebrates exceptional longevity.", 
    "Expresses gratitude for family and caregivers.", 
    "Fosters peace within self and others.", 
    "Appreciates established legacy.", 
    "Shares rare insights from extreme age.", 
    "Maintains a grateful mindset.", 
    "Accepts the full cycle of life.", 
    "Passes on last pieces of wisdom.",
    "Finds closure in achievements and contributions.", 
    "Reflects on a lifetime of milestones.", 
    "Relies on comprehensive support.", 
    "Embraces final peace."
]


# ------------------ ROUTES ------------------

@main.route("/")
def index():
    return render_template("index.html", focus_texts=focus_texts, checklist_items=checklist_items)

# Simple admin credentials
ADMIN_USERNAME = "Humanmaturogram"
ADMIN_PASSWORD = "@maturogram2025"

@main.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        data = request.get_json()
        username = data.get("username")
        password = data.get("password")

        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            session.permanent = True
            session["admin_logged_in"] = True
            return jsonify({"success": True, "redirect": url_for("admin")})
        else:
            return jsonify({"success": False, "error": "Wrong username or password."})

    return render_template("login.html")

@main.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))
def admin_required(func):
    """Decorator to protect admin routes."""
    from functools import wraps
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)
    return wrapper

@main.route("/submit", methods=["POST"])
def submit():
    try:
        age_str = request.form.get("age", "").strip()
        if not age_str:
            return "<div class='alert alert-warning'>Age is required.</div>"
        
        age = int(age_str)
        # Socio-demographic fields
        marital_status = request.form.get("marital_status", "").strip()
        gender = request.form.get("gender", "").strip()
        occupation = request.form.get("occupation", "").strip()
        education = request.form.get("education", "").strip()

        # Capture checklist responses (144 items)
        checklist_values = [
            int(request.form.get(f"l{i}_q{j}", 0))
            for i in range(1, 25)
            for j in range(1, 7)
        ]

        # Compute grouped totals per lustrum (24 groups)
        grouped_totals = [
            sum(checklist_values[i:i+6]) 
            for i in range(0, len(checklist_values), 6)
        ]

        # Calculate maturity scores
        calc = MaturographyCalculator(age, grouped_totals)
        result = calc.calculate()

        # Prepare row data (age + sociodemographic fields)
        insert_data = [age, marital_status, gender, occupation, education]

        # Append all checklist responses + grouped totals
        for i in range(24):
            insert_data.extend(checklist_values[i*6:(i+1)*6])  # 6 values
            insert_data.append(grouped_totals[i])               # total

        # Append Observed OHM
        insert_data.extend([
            result["Observed"]["a_ohm"],
            result["Observed"]["b_ohm"],
            result["Observed"]["c_ohm"],
            result["Observed"]["d_ohm"],
            result["Observed"]["ohm"]
        ])

        # Append Predicted PHM
        insert_data.extend([
            result["Predicted"]["a_phm"],
            result["Predicted"]["b_phm"],
            result["Predicted"]["c_phm"],
            result["Predicted"]["d_phm"],
            result["Predicted"]["phm"]
        ])

        # Percentage maturity + zone
        insert_data.append(result["percentage_hm"])
        insert_data.append(result["zone"])

        # -----------------------------------------
        # 🔥 FIXED: PostgreSQL uses %s placeholders, NOT ?
        # -----------------------------------------

        conn = get_db_connection()
        cur = conn.cursor()

        placeholders = ",".join(["%s"] * len(insert_data))

        cur.execute(
            f"INSERT INTO human_maturography_records ("
            "age, marital_status, gender, occupation, education, "
            + ",".join([
                # Generate dynamic columns for 24 lustra × 7 columns
                *(f"fg_{col}" for col in ["motor","language","interactions","emotional","curiosity","self_recognition","total"]),
                *(f"sa_{col}" for col in ["friendships","rules","empathy","self_regulation","independence","hobbies","total"]),
                *(f"id_{col}" for col in ["self_awareness","peer_interest","exploration","emotional_challenges","abstract_thinking","physical_changes","total"]),
                *(f"ir_{col}" for col in ["independence","responsibility","vocational","values","relationships","longterm_planning","total"]),
                *(f"cr_{col}" for col in ["transition_to_adult","focus_on_career","form_relationship","longterm_goals","manages_finances","explore_identity","total"]),
                *(f"fa_{col}" for col in ["establish_family","career_advancement","financial_planning","work_life_balance","build_home_env","support_networks","total"]),
                *(f"st_{col}" for col in ["stability_career","self_refinement","life_goal_adjustment","health_focus","work_life_balance","community_contribution","total"]),
                *(f"ml_{col}" for col in ["reflect_achievements","adjust_goals","focus_purpose","meaningful_activities","strengthen_relationships","address_challenges","total"]),
                *(f"er_{col}" for col in ["career_peak","mentorship_roles","children_focus","legacy_investment","community_service","balance_responsibilities","self_reflection","total"]),
                *(f"sa_{col}" for col in ["nurtures_others","personal_goals","creative_interests","work_life_harmony","future_preparation","share_wisdom","2_total"]),
                *(f"ws_{col}" for col in ["self_care","life_priority","mentoring_extensive","lifelong_learning","emotional_resilience","financial_planning","total"]),
                *(f"pr_{col}" for col in ["lifestyle_adjustment","meaningful_activities","health_management","legacy_building","career_transition","retirement","total"]),
                *(f"lr_{col}" for col in ["community_contribution","document_life","family_connections","reflect_achievements","social_engagement","emotional_stability","total"]),
                *(f"em_{col}" for col in ["mental_wellbeing","gratitude","life_acceptance","simple_joy","positive_outlook","counsel_others","total"]),
                *(f"wm_{col}" for col in ["offer_councel","find_peace","foster_resilience","shares_stories","spiritual_pursuits","meaningful_relationship","total"]),
                *(f"ar_{col}" for col in ["adapts_to_health","strengthen_relationships","pass_on_traditions","life_reflection","resilience_aging","maintain_independence","total"]),
                *(f"sr_{col}" for col in ["serenity_practices","life_reflection","family_milestones","quiet_pursuits","support_networks","sense_of_purpose","total"]),
                *(f"lf_{col}" for col in ["storytelling","inspire_future","spiritual_beliefs","family_connections","focus_legacy","accept_assistance","total"]),
                *(f"co_{col}" for col in ["inner_peace","simplicity","strengthen_bonds","express_gratitude","positive_memories","prioritize_wellbeing","total"]),
                *(f"rs_{col}" for col in ["final_reflection","preserve_memories","support_systems","spiritual_closure","share_wisdom","end_of_life","total"]),
                *(f"ex_{col}" for col in ["century_reflection","share_wisdom","family_unity","celebrate_centenarian","mental_engagement","historical_perspective","total"]),
                *(f"pa_{col}" for col in ["life_stages","dignity","meaningful_connections","daily_comfort","caregivers","memories_solace","total"]),
                *(f"gl_{col}" for col in ["celebrate_longevity","express_gratitude","foster_peace","appreciate_legacy","share_insights","grateful_mindset","total"]),
                *(f"fm_{col}" for col in ["accept_life_cycle","pass_wisdom","find_closure","lifetime_reflection","support_system","final_peace","total"]),
                "observed_lustrum", "observed_decade", "observed_generation", 
                "observed_life_stage", "observed_human_maturogram",
                "predicted_lustrum", "predicted_decade", "predicted_generation",
                "predicted_life_stage", "predicted_human_maturogram",
                "percentage_hm", "maturity_zone"
            ]) +
            ") VALUES (" + placeholders + ")",
            insert_data
        )

        conn.commit()
        cur.close()
        conn.close()

        # Return HTML result box (include socio-demographic summary)
        socio = {
            "age": age,
            "marital_status": marital_status,
            "gender": gender,
            "occupation": occupation,
            "education": education
        }
        return render_template("result_snippet.html", result=result, socio=socio)

    except Exception as e:
        return f"<div class='alert alert-danger'>Error: {str(e)}</div>"


@main.route("/admin")
@admin_required
def admin():
    if not session.get("admin_logged_in"):
        return redirect(url_for("login"))

    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM human_maturography_records", conn)
    finally:
        conn.close()

    if df.empty:
        table_html = "<p class='text-center text-muted'>No records available.</p>"
    else:
        # Build a column mapping for all columns
        DT_COLUMNS = {
            "id": "ID",
            "age": "Age",
            "marital_status": "Marital Status",
            "gender": "Gender",
            "occupation": "Occupation",
            "education": "Highest Education",
            # Foundational Growth
            "fg_motor": "Foundational Growth: Demonstrates basic motor skills",
            "fg_language": "Foundational Growth: Acquires language fundamentals",
            "fg_interactions": "Foundational Growth: Engages in initial interactions",
            "fg_emotional": "Foundational Growth: Forms emotional attachments",
            "fg_curiosity": "Foundational Growth: Exhibits curiosity about the environment",
            "fg_self_recognition": "Foundational Growth: Begins to recognize self in mirrors and responds to own name",
            "fg_total": "Foundational Growth: Total",
            # Social Awareness
            "sa_friendships": "Social Awareness: Forms friendships with peers",
            "sa_rules": "Social Awareness: Understands and follows basic rules in games",
            "sa_empathy": "Social Awareness: Shows basic empathy",
            "sa_self_regulation": "Social Awareness: Demonstrates self-regulation",
            "sa_independence": "Social Awareness: Exhibits growing independence",
            "sa_hobbies": "Social Awareness: Develops interests in hobbies or activities outside family influence",
            "sa_total": "Social Awareness: Total",
            # Identity Formation
            "id_self_awareness": "Identity Formation: Increases self-awareness",
            "id_peer_interest": "Identity Formation: Shows strong interest in peer groups and social acceptance",
            "id_exploration": "Identity Formation: Explores identity through appearance, interests, or beliefs",
            "id_emotional_challenges": "Identity Formation: Faces emotional challenges",
            "id_abstract_thinking": "Identity Formation: Develops abstract thinking",
            "id_physical_changes": "Identity Formation: Experiences physical changes associated with puberty",
            "id_total": "Identity Formation: Total",
            # Independence & Responsibility
            "ir_independence": "Independence and Responsibility: Pursues greater independence",
            "ir_responsibility": "Independence and Responsibility: Takes on early adult responsibilities",
            "ir_vocational": "Independence and Responsibility: Explores vocational interests",
            "ir_values": "Independence and Responsibility: Solidifies personal values and beliefs",
            "ir_relationships": "Independence and Responsibility: Builds romantic relationships",
            "ir_longterm_planning": "Independence and Responsibility: Develops long-term planning skills",
            "ir_total": "Independence and Responsibility: Total",
            # Career & Relationship Focus
            "cr_transition_to_adult": "Career and Relationship Focus: Transitions to full adult roles",
            "cr_focus_on_career": "Career and Relationship Focus: Focuses on career-building",
            "cr_form_relationship": "Career and Relationship Focus: Forms deeper personal relationships",
            "cr_longterm_goals": "Career and Relationship Focus: Begins considering long-term goals",
            "cr_manages_finances": "Career and Relationship Focus: Manages personal finances",  
            "cr_explore_identity": "Career and Relationship Focus: Explores self-identity in professional and social contexts",
            "cr_total": "Career and Relationship Focus: Total",
            #family and Career Advancement
            "fa_establish_family": "Family and Career Advancement: Establishes a family",
            "fa_career_advancement": "Family and Career Advancement: Advances in career",
            "fa_financial_planning": "Family and Career Advancement: Engages in financial planning",
            "fa_work_life_balance": "Family and Career Advancement: Balances multiple responsibilities",    
            "fa_build_home_env": "Family and Career Advancement: Builds a stable home environment",
            "fa_support_networks": "Family and Career Advancement: Develops stronger support networks", 
            "fa_total": "Family and Career Advancement: Total",
            # Stability and Growth
            "st_stability_career": "Stability and Growth: Seeks stability in career and personal relationships",
            "st_self_refinement": "Stability and Growth: Focuses on self-refinement",
            "st_life_goal_adjustment": "Stability and Growth: Reassesses life goals",
            "st_health_focus": "Stability and Growth: Prioritizes health and well-being",
            "st_work_life_balance": "Stability and Growth: Manages work-life balance",
            "st_community_contribution": "Stability and Growth: Contributes to community or professional networks",
            "st_total": "Stability and Growth: Total",
            # Mid-Life Reflection
            "ml_reflect_achievements": "Mid-Life Reflection: Reflects on past achievements",
            "ml_adjust_goals": "Mid-Life Reflection: Adjusts personal and professional goals",
            "ml_focus_purpose": "Mid-Life Reflection: Increases focus on life purpose",
            "ml_meaningful_activities": "Mid-Life Reflection: Pursues meaningful activities",
            "ml_strengthen_relationships": "Mid-Life Reflection: Strengthens family bonds and friendships",
            "ml_address_challenges": "Mid-Life Reflection: Addresses mid-life challenges",
            "ml_total": "Mid-Life Reflection: Total",
            # Expanded Responsibility
            "er_career_peak": "Expanded Responsibility: Reaches career peak",
            "er_mentorship_roles": "Expanded Responsibility: Takes on mentorship roles",
            "er_children_focus": "Expanded Responsibility: Focuses on children's development",
            "er_legacy_investment": "Expanded Responsibility: Invests in future legacy",
            "er_community_service": "Expanded Responsibility: Engages actively in community service",
            "er_balance_responsibilities": "Expanded Responsibility: Balances expanded responsibilities",
            "er_self_reflection": "Expanded Responsibility: Recognizes personal strengths through self-reflection",
            "er_total": "Expanded Responsibility: Total",
            # Self-Actualization
            "sa_nurtures_others": "Self-Actualization: Nurtures younger generations",
            "sa_personal_goals": "Self-Actualization: Achieves fulfillment in long-term personal goals",
            "sa_creative_interests": "Self-Actualization: Pursues creative or intellectual interests",
            "sa_work_life_harmony": "Self-Actualization: Maintains work-life harmony",
            "sa_future_preparation": "Self-Actualization: Prepares for future life transitions",
            "sa_share_wisdom": "Self-Actualization: Broadens influence by sharing wisdom",
            "sa2_total": "Self-Actualization: Total",   
            # Wisdom Sharing
            "ws_self_care": "Wisdom Sharing: Continues self-care routines", 
            "ws_life_priority": "Wisdom Sharing: Re-evaluates life priorities",
            "ws_mentoring_extensive": "Wisdom Sharing: Mentors others extensively", 
            "ws_lifelong_learning": "Wisdom Sharing: Engages in lifelong learning", 
            "ws_emotional_resilience": "Wisdom Sharing: Strengthens emotional resilience",
            "ws_financial_planning": "Wisdom Sharing: Plans financially for retirement",
            "ws_total": "Wisdom Sharing: Total",    
            # Preparation for Later Years
            "pr_lifestyle_adjustment": "Preparation for Later Years: Adjusts lifestyle for future needs",
            "pr_meaningful_activities": "Preparation for Later Years: Pursues meaningful activities",
            "pr_health_management": "Preparation for Later Years: Manages health proactively",  
            "pr_legacy_building": "Preparation for Later Years: Builds legacy through family or community contributions",
            "pr_career_transition": "Preparation for Later Years: Transitions career towards reduced hours",
            "pr_retirement": "Preparation for Later Years: Enters retirement or reduces workload",  
            "pr_total": "Preparation for Later Years: Total",
            # Legacy and Reflection 
            "lr_community_contribution": "Legacy and Reflection: Contributes via community service",
            "lr_document_life": "Legacy and Reflection: Writes memoirs or documents life experiences",
            "lr_family_connections": "Legacy and Reflection: Enjoys deeper family connections", 
            "lr_reflect_achievements": "Legacy and Reflection: Reflects on life achievements",
            "lr_social_engagement": "Legacy and Reflection: Maintains social engagements",  
            "lr_emotional_stability": "Legacy and Reflection: Focuses on emotional stability",
            "lr_total": "Legacy and Reflection: Total",
            # Emotional Balance
            "em_mental_wellbeing": "Emotional Balance: Prioritizes mental well-being",
            "em_gratitude": "Emotional Balance: Fosters gratitude", 
            "em_life_acceptance": "Emotional Balance: Accepts life's stages",
            "em_simple_joy": "Emotional Balance: Finds joy in simpler aspects",
            "em_positive_outlook": "Emotional Balance: Maintains positive outlook",
            "em_counsel_others": "Emotional Balance: Offers counsel and mentorship",
            "em_total": "Emotional Balance: Total",
            # Wisdom and Mentorship
            "wm_offer_councel":"Offers counsel to younger members",
            "wm_find_peace": "Finds peace with accomplishments",
            "wm_foster_resilience": "Fosters personal resilience",
            "wm_shares_stories": "Shares stories and lessons",
            "wm_spiritual_pursuits": ": Engages in spiritual pursuits",
            "wm_meaningful_relationship": "Prioritizes meaningful relationships",
            "wm_total": "Wisdom and Mentorship: Total",
            # Acceptance and Resilience
            "ar_adapts_to_health": "Acceptance and Resilience: Strengthens key relationships",
            "ar_strengthen_relationships": "Acceptance and Resilience: Passes on traditions and cultural knowledge",
            "ar_pass_on_traditions": "Acceptance and Resilience: Appreciates life's journey",
            "ar_life_reflection": "Acceptance and Resilience: Builds resilience against aging challenges",
            "ar_resilience_aging": "Acceptance and Resilience: Maintains independence", 
            "ar_maintain_independence": "Acceptance and Resilience: Total",
            "ar_total": "Acceptance and Resilience: Total", 
            # Serenity and Reflection
            "sr_serenity_practices": "Serenity and Reflection: Reflects deeply on life experiences",
            "sr_life_reflection": "Serenity and Reflection: Celebrates family milestones",  
            "sr_family_milestones": "Serenity and Reflection: Enjoys quiet pursuits",
            "sr_quiet_pursuits": "Serenity and Reflection: Relies on support networks",
            "sr_support_networks": "Serenity and Reflection: Maintains a sense of purpose",
            "sr_sense_of_purpose": "Serenity and Reflection: Total",    
            "sr_total": "Serenity and Reflection: Total",
            # Legacy Fulfillment    
            "lf_storytelling": "Legacy Fulfillment: Engages in storytelling to preserve history",
            "lf_inspire_future": "Legacy Fulfillment: Inspires future generations", 
            "lf_spiritual_beliefs": "Legacy Fulfillment: Relies on spiritual beliefs",
            "lf_family_connections": "Legacy Fulfillment: Deepens family connections",
            "lf_focus_legacy": "Legacy Fulfillment: Focuses on legacy fulfillment",
            "lf_accept_assistance": "Legacy Fulfilment: Accepts assistance gracefully",
            "lf_total": "Legacy Fulfillment: Total",
            # Contentment and Inner Peace
            "co_inner_peace": "Contentment and Inner Peace: Emphasizes inner peace through mindfulness",
            "co_simplicity": "Contentment and Inner Peace: Embraces simplicity",
            "co_strengthen_bonds": "Contentment and Inner Peace: Strengthens bonds with loved ones",
            "co_express_gratitude": "Contentment and Inner Peace: Expresses gratitude",
            "co_positive_memories": "Contentment and Inner Peace: Reflects on positive memories",
            "co_prioritize_wellbeing": "Contentment and Inner Peace: Prioritizes comfort and well-being",
            "co_total": "Contentment and Inner Peace: Total",   
            # Completion of Legacy
            "rs_final_reflection": "Completion of Legacy: Engages in final reflections",    
            "rs_preserve_memories": "Completion of Legacy: Preserves memories",
            "rs_support_systems": "Completion of Legacy: Relies on support systems",    
            "rs_spiritual_closure": "Completion of Legacy: Seeks spiritual closure",
            "rs_share_wisdom": "Completion of Legacy: Shares final wisdom", 
            "rs_end_of_life": "Completion of Legacy: Accepts end-of-life transitions",
            "rs_total": "Completion of Legacy: Total",
            # Extended Reflection
            "ex_century_reflection": "Extended Reflection: Reflects on a century's worth of experiences",
            "ex_share_wisdom": "Extended Reflection: Shares profound life wisdom",
            "ex_family_unity": "Extended Reflection: Fosters family unity", 
            "ex_celebrate_centenarian": "Extended Reflection: Celebrates centenarian status",
            "ex_mental_engagement": "Extended Reflection: Maintains mental engagement",
            "ex_historical_perspective": "Extended Reflection: Appreciates historical perspectives",
            "ex_total": "Extended Reflection: Total",
            # Peaceful Acceptance
            "pa_life_stages": "Peaceful Acceptance: Embraces life's final stages peacefully",
            "pa_dignity": "Peaceful Acceptance: Focuses on maintaining dignity",    
            "pa_meaningful_connections": "Peaceful Acceptance: Prioritizes meaningful connections",
            "pa_daily_comfort": "Peaceful Acceptance: Seeks comfort in daily routines",
            "pa_caregivers": "Peaceful Acceptance: Relies heavily on caregivers",
            "pa_memories_solace": "Peaceful Acceptance: Finds solace in memories",  
            "pa_total": "Peaceful Acceptance: Total",
            # Gratitude for Longevity
            "gl_celebrate_longevity": "Gratitude for Longevity: Celebrates exceptional longevity",
            "gl_express_gratitude": "Gratitude for Longevity: Expresses gratitude for family and caregivers",
            "gl_foster_peace": "Gratitude for Longevity: Fosters peace within self and others",
            "gl_appreciate_legacy": "Gratitude for Longevity: Appreciates established legacy",
            "gl_share_insights": "Gratitude for Longevity: Shares rare insights from extreme age",
            "gl_grateful_mindset": "Gratitude for Longevity: Maintains a grateful mindset",
            "gl_total": "Gratitude for Longevity: Total",
            # Final Milestones
            "fm_accept_life_cycle": "Final Milestones: Accepts the full cycle of life",
            "fm_pass_wisdom": "Final Milestones: Passes on last pieces of wisdom",
            "fm_find_closure": "Final Milestones: Finds closure in achievements",   
            "fm_lifetime_reflection": "Final Milestones: Reflects on a lifetime of milestones",
            "fm_support_system": "Final Milestones: Relies on comprehensive support",
            "fm_final_peace": "Final Milestones: Embraces final peace",
            "fm_total": "Final Milestones: Total",
            # Observed/Predicted
            "observed_lustrum": "Observed Lustrum",
            "observed_decade": "Observed Decade",
            "observed_generation": "Observed Generation",
            "observed_life_stage": "Observed Life Stage",
            "observed_human_maturogram": "Observed Human Maturogram",
            "predicted_lustrum": "Predicted Lustrum",
            "predicted_decade": "Predicted Decade",
            "predicted_generation": "Predicted Generation",
            "predicted_life_stage": "Predicted Life Stage",
            "predicted_human_maturogram": "Predicted Human Maturogram",
            "percentage_hm": "Percentage Human Maturogram",
            "maturity_zone": "Maturity zone"
        }
        
        # Helper to convert a psycopg2 row to dict
        def row_to_dict(cur, row):
            return {desc[0]: row[idx] for idx, desc in enumerate(cur.description)} if hasattr(cur, 'description') else dict(row)


        # Admin page (keeps rendering same admin.html)
        @main.route("/admin")
        @admin_required
        def admin():
            # admin.html will load data via AJAX from /admin_data
            return render_template("admin.html")

        # Server-side DataTables endpoint
        @main.route("/admin_data", methods=["GET", "POST"])
        @admin_required
        def admin_data():
            # DataTables uses parameters like draw, start, length, search[value], order...
            params = request.values

            draw = int(params.get("draw", 1))
            start = int(params.get("start", 0))
            length = int(params.get("length", 10))
            search_value = params.get("search[value]", "").strip()

            # Ordering
            order_col_index = params.get("order[0][column]")
            order_col_dir = params.get("order[0][dir]", "asc")
            order_col = None
            if order_col_index is not None:
                try:
                    order_col_index = int(order_col_index)
                    # Map index to column name as we send DT_COLUMNS to the client in same order
                    if 0 <= order_col_index < len(DT_COLUMNS):
                        order_col = DT_COLUMNS[order_col_index]
                except:
                    order_col = None

            # Build base query
            select_cols = ", ".join(DT_COLUMNS)
            base_query = f"SELECT {select_cols} FROM human_maturography_records"

            # Count total records
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(1) FROM human_maturography_records")
            total_records = cur.fetchone()[0]

            # Filtering
            where_clause = ""
            values = []
            if search_value:
                # we'll search a few meaningful columns (age, maturity_zone, percentage_hm)
                where_clause = " WHERE (CAST(age AS TEXT) ILIKE %s OR maturity_zone ILIKE %s OR CAST(percentage_hm AS TEXT) ILIKE %s)"
                sv = f"%{search_value}%"
                values.extend([sv, sv, sv])

            # Count filtered
            count_query = "SELECT COUNT(1) FROM human_maturography_records" + where_clause
            cur.execute(count_query, values)
            filtered_records = cur.fetchone()[0]

            # Ordering & Pagination
            order_sql = ""
            if order_col:
                # safety: order_col must be valid and from our whitelist
                order_sql = f" ORDER BY {order_col} {'DESC' if order_col_dir == 'desc' else 'ASC'}"
            limit_sql = " LIMIT %s OFFSET %s"
            values.extend([length, start])

            final_query = base_query + where_clause + order_sql + limit_sql

            cur.execute(final_query, values)
            rows = cur.fetchall()

            data = []
            for row in rows:
                rowd = row_to_dict(cur, row)
                # Keep order consistent with DT_COLUMNS (so client can render columns easily)
                data.append([rowd.get(c) for c in DT_COLUMNS])

            # Build response according to DataTables server-side spec
            response = {
                "draw": draw,
                "recordsTotal": total_records,
                "recordsFiltered": filtered_records,
                "data": data
            }

            cur.close()
            conn.close()
            return jsonify(response)
    # Rename dataframe columns for display
        df_display = df.rename(columns=DT_COLUMNS)
        table_html = df_display.to_html(classes="table table-striped table-bordered", index=False, escape=False)

    # Finally, render template with table
    return render_template("admin.html", table_html=table_html)

@main.route("/download")
def download():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM human_maturography_records", conn)
    conn.close()
    file_path = "human_maturography_records.xlsx"
    df.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    main.run(host="0.0.0.0", port=port)

