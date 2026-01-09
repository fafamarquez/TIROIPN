import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, redirect, url_for, flash
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()
migrate = Migrate()

login_manager = LoginManager()
login_manager.login_view = "login"  # a dónde manda si no hay sesión

class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(60), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, nullable=False, default=False)

    def set_password(self, raw_password: str) -> None:
        self.password_hash = generate_password_hash(raw_password)

    def check_password(self, raw_password: str) -> bool:
        return check_password_hash(self.password_hash, raw_password)

class Miembro(db.Model):
    __tablename__ = "miembros"

    miembro_id = db.Column(db.Integer, primary_key=True)


    nombre = db.Column(db.String(120), nullable=False)
    apellido_paterno = db.Column(db.String(80))
    apellido_materno = db.Column(db.String(80))

   
    curp = db.Column(db.String(18), unique=True, nullable=False)
    correo = db.Column(db.String(120))
    celular = db.Column(db.String(20))
    edad = db.Column(db.SmallInteger)
    alergias = db.Column(db.Text)
    fecha_registro = db.Column(db.DateTime, nullable=False)

    coach = db.relationship(
        "Coach",
        back_populates="miembro",
        uselist=False,
        cascade="all, delete-orphan",
    )


class Coach(db.Model):
    __tablename__ = "coachs"

    miembro_id = db.Column(
        db.Integer,
        db.ForeignKey("miembros.miembro_id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
    )

    miembro = db.relationship("Miembro", back_populates="coach")

class Clase(db.Model):
    __tablename__ = "clases"

    clase_id = db.Column(db.Integer, primary_key=True)
    dias = db.Column(db.String(40))
    hora_inicio = db.Column(db.Time, nullable=False)
    hora_fin = db.Column(db.Time, nullable=False)
    nivel = db.Column(db.String(30), nullable=False)
    coach_id = db.Column(
        db.Integer,
        db.ForeignKey("coachs.miembro_id", ondelete="RESTRICT", onupdate="CASCADE"),
        nullable=False,
    )

    coach = db.relationship("Coach")

class Atleta(db.Model):
    __tablename__ = "atletas"

    miembro_id = db.Column(
        db.Integer,
        db.ForeignKey("miembros.miembro_id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True,
    )
    boleta = db.Column(db.String(20), unique=True)
    alumno_ipn = db.Column(db.Boolean, nullable=False, default=False)
    nivel = db.Column(db.String(30), nullable=False, default="Inicial")
    clase_id = db.Column(
        db.Integer,
        db.ForeignKey("clases.clase_id", ondelete="RESTRICT", onupdate="CASCADE"),
        nullable=False,
    )

    miembro = db.relationship("Miembro")
    clase = db.relationship("Clase")

class Arco(db.Model):
    __tablename__ = "arcos"

    arco_id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.String(30), nullable=False)
    libraje = db.Column(db.SmallInteger, nullable=False)
    mano = db.Column(db.String(10), nullable=False)
    estabilizador = db.Column(db.Boolean, nullable=False, default=False)
    mira = db.Column(db.Boolean, nullable=False, default=False)
    rama = db.Column(db.String(40))
    maneral = db.Column(db.String(40))

    miembro_id = db.Column(
        db.Integer,
        db.ForeignKey("miembros.miembro_id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
    )

    miembro = db.relationship("Miembro")

def create_app():
    load_dotenv()

    app = Flask(__name__)
    login_manager.init_app(app)

    @app.cli.command("create-admin")
    def create_admin():
        username = os.getenv("ADMIN_USERNAME", "admin")
        password = os.getenv("ADMIN_PASSWORD", "admin123")

        if User.query.filter_by(username=username).first():
            print("Admin ya existe.")
            return

        u = User(username=username, is_admin=True)
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        print("Admin creado:", username)

    @app.cli.command("reset-admin-password")
    def reset_admin_password():
        username = os.getenv("ADMIN_USERNAME", "admin")
        password = os.getenv("ADMIN_PASSWORD", "admin123")

        user = User.query.filter_by(username=username).first()
        if not user:
            print("No existe el usuario:", username)
            return

        user.set_password(password)
        db.session.commit()
        print("Password actualizado para:", username)


    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Falta DATABASE_URL en el archivo .env")

    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    db.init_app(app)
    migrate.init_app(app, db)

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")

            user = User.query.filter_by(username=username).first()
            if not user or not user.check_password(password):
                flash("Usuario o contraseña inválidos.", "error")
                return render_template("login.html")

            login_user(user)
            flash("Sesión iniciada.", "success")
            return redirect(url_for("index"))

        return render_template("login.html")

    @app.get("/logout")
    @login_required
    def logout():
        logout_user()
        flash("Sesión cerrada.", "success")
        return redirect(url_for("login"))

    @app.get("/")
    @login_required
    def index():
        return render_template("index.html")


    @app.get("/health")
    @login_required
    def health():
        with db.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify(ok=True, db="connected")

    @app.get("/coachs")
    @login_required
    def list_coachs():
        coachs = (
            db.session.query(Coach, Miembro)
            .join(Miembro, Miembro.miembro_id == Coach.miembro_id)
            .order_by(Coach.miembro_id.desc())
            .all()
        )
        return render_template("coachs_list.html", coachs=coachs)

    @app.route("/coachs/new", methods=["GET", "POST"])
    @login_required
    def new_coach():
        if request.method == "POST":
            nombre = request.form.get("nombre", "").strip()
            ap_pat = request.form.get("apellido_paterno", "").strip() or None
            ap_mat = request.form.get("apellido_materno", "").strip() or None

            curp = request.form.get("curp", "").strip().upper()
            correo = request.form.get("correo", "").strip() or None
            celular = request.form.get("celular", "").strip() or None
            edad_raw = request.form.get("edad", "").strip()
            alergias = request.form.get("alergias", "").strip() or None

            edad = int(edad_raw) if edad_raw else None

            if not nombre:
                flash("El nombre es obligatorio.", "error")
                return render_template("coachs_new.html")

            if not curp:
                flash("CURP es obligatorio.", "error")
                return render_template("coachs_new.html")

            try:
       
                miembro = Miembro.query.filter_by(curp=curp).first()

                if miembro is None:
             
                    miembro = Miembro(
                        nombre=nombre,
                        apellido_paterno=ap_pat,
                        apellido_materno=ap_mat,
                        curp=curp,
                        correo=correo,
                        celular=celular,
                        edad=edad,
                        alergias=alergias,
                        fecha_registro=datetime.now(),
                    )
                    db.session.add(miembro)
                    db.session.flush() 

                else:
                   
                    if not miembro.nombre and nombre:
                        miembro.nombre = nombre
                    if not miembro.apellido_paterno and ap_pat:
                        miembro.apellido_paterno = ap_pat
                    if not miembro.apellido_materno and ap_mat:
                        miembro.apellido_materno = ap_mat
                    if not miembro.correo and correo:
                        miembro.correo = correo
                    if not miembro.celular and celular:
                        miembro.celular = celular
                    if miembro.edad is None and edad is not None:
                        miembro.edad = edad
                    if not miembro.alergias and alergias:
                        miembro.alergias = alergias

                ya_es_coach = Coach.query.filter_by(miembro_id=miembro.miembro_id).first()
                if ya_es_coach:
                    db.session.rollback()
                    flash("Ese miembro ya está registrado como coach.", "error")
                    return redirect(url_for("list_coachs"))

   
                db.session.add(Coach(miembro_id=miembro.miembro_id))
                db.session.commit()

                flash("Coach creado correctamente.", "success")
                return redirect(url_for("list_coachs"))

            except IntegrityError:
                db.session.rollback()
                flash("Error: CURP duplicada o algún dato viola restricciones (CURP/celular/edad/correo).", "error")
                return render_template("coachs_new.html")

            except Exception as e:
                db.session.rollback()
                flash(f"Error inesperado: {e.__class__.__name__}", "error")
                return render_template("coachs_new.html")

        return render_template("coachs_new.html")
    
    @app.route("/coachs/<int:miembro_id>/edit", methods=["GET", "POST"])
    @login_required
    def edit_coach(miembro_id):
        miembro = Miembro.query.get_or_404(miembro_id)
        coach = Coach.query.filter_by(miembro_id=miembro_id).first()
        if not coach:
            flash("Ese miembro no tiene rol coach.", "error")
            return redirect(url_for("list_coachs"))

        if request.method == "POST":
            miembro.nombre = request.form.get("nombre", "").strip()
            miembro.apellido_paterno = request.form.get("apellido_paterno", "").strip() or None
            miembro.apellido_materno = request.form.get("apellido_materno", "").strip() or None
            miembro.correo = request.form.get("correo", "").strip() or None
            miembro.celular = request.form.get("celular", "").strip() or None

            edad_raw = request.form.get("edad", "").strip()
            miembro.edad = int(edad_raw) if edad_raw else None
            miembro.alergias = request.form.get("alergias", "").strip() or None

            try:
                db.session.commit()
                flash("Coach actualizado.", "success")
                return redirect(url_for("list_coachs"))
            except Exception as e:
                db.session.rollback()
                flash(f"Error al actualizar: {e.__class__.__name__}", "error")

        return render_template("coachs_edit.html", miembro=miembro)

    @app.post("/coachs/<int:miembro_id>/delete")
    @login_required
    def delete_coach(miembro_id):
        coach = Coach.query.filter_by(miembro_id=miembro_id).first()
        if not coach:
            flash("Coach no encontrado.", "error")
            return redirect(url_for("list_coachs"))

        es_atleta = Atleta.query.filter_by(miembro_id=miembro_id).first() is not None
        if not es_atleta:
            flash("No se puede eliminar el rol coach: el miembro quedaría sin rol (ISA total).", "error")
            return redirect(url_for("list_coachs"))

        try:
            db.session.delete(coach)
            db.session.commit()
            flash("Rol coach eliminado.", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"No se pudo eliminar: {e.__class__.__name__}", "error")

        return redirect(url_for("list_coachs"))



    @app.get("/clases")
    @login_required
    def list_clases():
        clases = (
            db.session.query(Clase, Coach, Miembro)
            .join(Coach, Coach.miembro_id == Clase.coach_id)
            .join(Miembro, Miembro.miembro_id == Coach.miembro_id)
            .order_by(Clase.clase_id.desc())
            .all()
        )
        return render_template("clases_list.html", clases=clases)

    @app.route("/clases/new", methods=["GET", "POST"])
    @login_required
    def new_clase():
        coachs = (
            db.session.query(Coach, Miembro)
            .join(Miembro, Miembro.miembro_id == Coach.miembro_id)
            .order_by(Miembro.nombre.asc())
            .all()
        )

        if request.method == "POST":
            dias = request.form.get("dias", "").strip() or None
            hora_inicio = request.form.get("hora_inicio", "").strip()
            hora_fin = request.form.get("hora_fin", "").strip()
            nivel = request.form.get("nivel", "Inicial").strip()
            coach_id_raw = request.form.get("coach_id", "").strip()

            if not hora_inicio or not hora_fin:
                flash("Hora inicio y hora fin son obligatorias.", "error")
                return render_template("clases_new.html", coachs=coachs)

            if nivel not in ("Inicial", "Intermedio", "Avanzado"):
                flash("Nivel inválido.", "error")
                return render_template("clases_new.html", coachs=coachs)

            if not coach_id_raw.isdigit():
                flash("Debes seleccionar un coach.", "error")
                return render_template("clases_new.html", coachs=coachs)

     
            try:
                hi_h, hi_m = hora_inicio.split(":")
                hf_h, hf_m = hora_fin.split(":")
                hi = datetime.strptime(f"{hi_h}:{hi_m}", "%H:%M").time()
                hf = datetime.strptime(f"{hf_h}:{hf_m}", "%H:%M").time()
            except Exception:
                flash("Formato de hora inválido. Usa HH:MM.", "error")
                return render_template("clases_new.html", coachs=coachs)

            if hf <= hi:
                flash("Hora fin debe ser mayor que hora inicio.", "error")
                return render_template("clases_new.html", coachs=coachs)

            coach_id = int(coach_id_raw)

            try:
                clase = Clase(
                    dias=dias,
                    hora_inicio=hi,
                    hora_fin=hf,
                    nivel=nivel,
                    coach_id=coach_id,
                )
                db.session.add(clase)
                db.session.commit()
                flash("Clase creada correctamente.", "success")
                return redirect(url_for("list_clases"))
            except Exception as e:
                db.session.rollback()
                flash(f"Error al crear clase: {e.__class__.__name__}", "error")
                return render_template("clases_new.html", coachs=coachs)

        return render_template("clases_new.html", coachs=coachs)

    @app.route("/clases/<int:clase_id>/edit", methods=["GET", "POST"])
    @login_required
    def edit_clase(clase_id):
        clase = Clase.query.get_or_404(clase_id)
        coachs = Coach.query.all()

        if request.method == "POST":
            clase.dias = request.form.get("dias", "").strip() or None
            clase.hora_inicio = request.form.get("hora_inicio")
            clase.hora_fin = request.form.get("hora_fin")
            clase.nivel = request.form.get("nivel", "Inicial").strip()

            coach_id_raw = request.form.get("coach_id", "").strip()
            if not coach_id_raw.isdigit():
                flash("Debes seleccionar un coach.", "error")
                return render_template("clases_edit.html", clase=clase, coachs=coachs)

            clase.coach_id = int(coach_id_raw)

            try:
                db.session.commit()
                flash("Clase actualizada.", "success")
                return redirect(url_for("list_clases"))
            except Exception as e:
                db.session.rollback()
                flash(f"Error al actualizar: {e.__class__.__name__}", "error")

        return render_template("clases_edit.html", clase=clase, coachs=coachs)


    @app.post("/clases/<int:clase_id>/delete")
    @login_required
    def delete_clase(clase_id):
        clase = Clase.query.get_or_404(clase_id)

     
        hay_atletas = Atleta.query.filter_by(clase_id=clase_id).first() is not None
        if hay_atletas:
            flash("No se puede eliminar: hay atletas asignados a esta clase.", "error")
            return redirect(url_for("list_clases"))

        try:
            db.session.delete(clase)
            db.session.commit()
            flash("Clase eliminada.", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"No se pudo eliminar: {e.__class__.__name__}", "error")

        return redirect(url_for("list_clases"))

    @app.get("/api/clases")
    @login_required
    def api_clases_por_nivel():
        nivel = (request.args.get("nivel") or "").strip()
        if nivel not in ("Inicial", "Intermedio", "Avanzado"):
            return {"ok": False, "items": [], "error": "Nivel inválido"}, 400

        clases = (
            Clase.query
            .filter(Clase.nivel == nivel)
            .order_by(Clase.hora_inicio.asc(), Clase.clase_id.asc())
            .all()
        )

        items = [
            {
                "clase_id": c.clase_id,
                "label": f"ID {c.clase_id} — {c.dias or ''} {c.hora_inicio}-{c.hora_fin} (Coach {c.coach_id})".strip()
            }
            for c in clases
        ]
        return {"ok": True, "items": items}


    @app.get("/atletas")
    @login_required
    def list_atletas():
        atletas = (
            db.session.query(Atleta, Miembro, Clase)
            .join(Miembro, Miembro.miembro_id == Atleta.miembro_id)
            .join(Clase, Clase.clase_id == Atleta.clase_id)
            .order_by(Atleta.miembro_id.desc())
            .all()
        )
        return render_template("atletas_list.html", atletas=atletas)

    @app.route("/atletas/new", methods=["GET", "POST"])
    @login_required
    def new_atleta():
        
        nivel_sel = (
            request.form.get("nivel", "Inicial").strip()
            if request.method == "POST"
            else request.args.get("nivel", "Inicial").strip()
        )

        if nivel_sel not in ("Inicial", "Intermedio", "Avanzado"):
            nivel_sel = "Inicial"

        
        clases = (
            Clase.query
            .filter_by(nivel=nivel_sel)
            .order_by(Clase.clase_id.desc())
            .all()
        )

        if request.method == "POST":
           
            nombre = request.form.get("nombre", "").strip()
            ap_pat = request.form.get("apellido_paterno", "").strip() or None
            ap_mat = request.form.get("apellido_materno", "").strip() or None

            curp = request.form.get("curp", "").strip().upper()
            correo = request.form.get("correo", "").strip() or None
            celular = request.form.get("celular", "").strip() or None
            edad_raw = request.form.get("edad", "").strip()
            alergias = request.form.get("alergias", "").strip() or None

            edad = int(edad_raw) if edad_raw else None

         
            boleta = request.form.get("boleta", "").strip() or None
            alumno_ipn = True if request.form.get("alumno_ipn") == "on" else False
            nivel = request.form.get("nivel", "Inicial").strip()
            clase_id_raw = request.form.get("clase_id", "").strip()

          
            if not nombre:
                flash("El nombre es obligatorio.", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

            if not curp:
                flash("CURP es obligatorio.", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

            if nivel not in ("Inicial", "Intermedio", "Avanzado"):
                flash("Nivel inválido.", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

            if not clase_id_raw.isdigit():
                flash("Debes seleccionar una clase.", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

            clase_id = int(clase_id_raw)

    
            if alumno_ipn and not boleta:
                flash("Si es alumno IPN, la boleta es obligatoria.", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

            try:
           
                clase = Clase.query.get(clase_id)
                if not clase:
                    flash("La clase seleccionada no existe.", "error")
                    return render_template("atletas_new.html", clases=clases, nivel_sel=nivel)

                if clase.nivel != nivel:
                    flash("La clase seleccionada no corresponde al nivel del atleta.", "error")
                    return render_template("atletas_new.html", clases=clases, nivel_sel=nivel)

         
                miembro = Miembro.query.filter_by(curp=curp).first()

                if miembro is None:
                    miembro = Miembro(
                        nombre=nombre,
                        apellido_paterno=ap_pat,
                        apellido_materno=ap_mat,
                        curp=curp,
                        correo=correo,
                        celular=celular,
                        edad=edad,
                        alergias=alergias,
                        fecha_registro=datetime.now(),
                    )
                    db.session.add(miembro)
                    db.session.flush()
                else:
     
                    if not miembro.nombre and nombre:
                        miembro.nombre = nombre
                    if not miembro.apellido_paterno and ap_pat:
                        miembro.apellido_paterno = ap_pat
                    if not miembro.apellido_materno and ap_mat:
                        miembro.apellido_materno = ap_mat
                    if not miembro.correo and correo:
                        miembro.correo = correo
                    if not miembro.celular and celular:
                        miembro.celular = celular
                    if miembro.edad is None and edad is not None:
                        miembro.edad = edad
                    if not miembro.alergias and alergias:
                        miembro.alergias = alergias

     
                ya_es_atleta = Atleta.query.filter_by(miembro_id=miembro.miembro_id).first()
                if ya_es_atleta:
                    db.session.rollback()
                    flash("Ese miembro ya está registrado como atleta.", "error")
                    return redirect(url_for("list_atletas"))

                atleta = Atleta(
                    miembro_id=miembro.miembro_id,
                    boleta=boleta,
                    alumno_ipn=alumno_ipn,
                    nivel=nivel,
                    clase_id=clase_id,
                )
                db.session.add(atleta)
                db.session.commit()

                flash("Atleta creado correctamente.", "success")
                return redirect(url_for("list_atletas"))

            except IntegrityError:
                db.session.rollback()
                flash("Error: boleta duplicada o datos inválidos (CURP/boleta/nivel).", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

            except Exception as e:
                db.session.rollback()
                flash(f"Error inesperado: {e.__class__.__name__}", "error")
                return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

        return render_template("atletas_new.html", clases=clases, nivel_sel=nivel_sel)

    @app.route("/atletas/<int:miembro_id>/edit", methods=["GET", "POST"])
    @login_required
    def edit_atleta(miembro_id):
        miembro = Miembro.query.get_or_404(miembro_id)
        atleta = Atleta.query.filter_by(miembro_id=miembro_id).first()
        if not atleta:
            flash("Ese miembro no tiene rol atleta.", "error")
            return redirect(url_for("list_atletas"))

        if request.method == "POST":

            miembro.nombre = request.form.get("nombre", "").strip()
            miembro.apellido_paterno = request.form.get("apellido_paterno", "").strip() or None
            miembro.apellido_materno = request.form.get("apellido_materno", "").strip() or None
            miembro.correo = request.form.get("correo", "").strip() or None
            miembro.celular = request.form.get("celular", "").strip() or None

            edad_raw = request.form.get("edad", "").strip()
            miembro.edad = int(edad_raw) if edad_raw else None
            miembro.alergias = request.form.get("alergias", "").strip() or None

            atleta.alumno_ipn = True if request.form.get("alumno_ipn") else False
            atleta.boleta = request.form.get("boleta", "").strip() or None
            atleta.nivel = request.form.get("nivel", "Inicial").strip()

            clase_id_raw = request.form.get("clase_id", "").strip()
            if not clase_id_raw.isdigit():
                flash("Debes seleccionar una clase.", "error")
                return render_template("atletas_edit.html", miembro=miembro, atleta=atleta)

            atleta.clase_id = int(clase_id_raw)

            try:
                db.session.commit()
                flash("Atleta actualizado.", "success")
                return redirect(url_for("list_atletas"))
            except Exception as e:
                db.session.rollback()
                flash(f"Error al actualizar: {e.__class__.__name__}", "error")

        return render_template("atletas_edit.html", miembro=miembro, atleta=atleta)


    @app.post("/atletas/<int:miembro_id>/delete")
    @login_required
    def delete_atleta(miembro_id):
        atleta = Atleta.query.filter_by(miembro_id=miembro_id).first()
        if not atleta:
            flash("Atleta no encontrado.", "error")
            return redirect(url_for("list_atletas"))

        try:
            db.session.delete(atleta)
            db.session.commit()
            flash("Rol atleta eliminado.", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"No se pudo eliminar: {e.__class__.__name__}", "error")

        return redirect(url_for("list_atletas"))


    @app.get("/arcos")
    @login_required
    def list_arcos():
        arcos = (
            db.session.query(Arco, Miembro)
            .join(Miembro, Miembro.miembro_id == Arco.miembro_id)
            .order_by(Arco.arco_id.desc())
            .all()
        )
        return render_template("arcos_list.html", arcos=arcos)

    @app.route("/arcos/new", methods=["GET", "POST"])
    @login_required
    def new_arco():
        miembros = (
            Miembro.query
            .order_by(Miembro.miembro_id.desc())
            .all()
        )

        if request.method == "POST":
            tipo = request.form.get("tipo", "recurvo").strip()
            mano = request.form.get("mano", "diestro").strip()
            rama = request.form.get("rama", "").strip() or None
            maneral = request.form.get("maneral", "").strip() or None

            libraje_raw = request.form.get("libraje", "").strip()
            miembro_id_raw = request.form.get("miembro_id", "").strip()

            estabilizador = True if request.form.get("estabilizador") == "on" else False
            mira = True if request.form.get("mira") == "on" else False

            if tipo not in ("recurvo", "compuesto", "barebow", "tradicional"):
                flash("Tipo inválido.", "error")
                return render_template("arcos_new.html", miembros=miembros)

            if mano not in ("diestro", "zurdo"):
                flash("Mano inválida.", "error")
                return render_template("arcos_new.html", miembros=miembros)

            if not libraje_raw.isdigit():
                flash("Libraje debe ser un número.", "error")
                return render_template("arcos_new.html", miembros=miembros)

            libraje = int(libraje_raw)
            if libraje < 10 or libraje > 70:
                flash("Libraje debe estar entre 10 y 70.", "error")
                return render_template("arcos_new.html", miembros=miembros)

            if not miembro_id_raw.isdigit():
                flash("Debes seleccionar un miembro.", "error")
                return render_template("arcos_new.html", miembros=miembros)

            miembro_id = int(miembro_id_raw)

            try:
                arco = Arco(
                    tipo=tipo,
                    libraje=libraje,
                    mano=mano,
                    estabilizador=estabilizador,
                    mira=mira,
                    rama=rama,
                    maneral=maneral,
                    miembro_id=miembro_id,
                )
                db.session.add(arco)
                db.session.commit()
                flash("Arco registrado correctamente.", "success")
                return redirect(url_for("list_arcos"))

            except Exception as e:
                db.session.rollback()
                flash(f"Error al registrar arco: {e.__class__.__name__}", "error")
                return render_template("arcos_new.html", miembros=miembros)

        return render_template("arcos_new.html", miembros=miembros)

    @app.get("/dashboard")
    @login_required
    def dashboard():
        db.session.execute(text("""
            INSERT INTO dw.dim_miembros (miembro_id, nombre_completo, curp, es_atleta, es_coach)
            SELECT 
                m.miembro_id, 
                (m.nombre || ' ' || COALESCE(m.apellido_paterno, '')), 
                m.curp,
                EXISTS(SELECT 1 FROM atletas a WHERE a.miembro_id = m.miembro_id),
                EXISTS(SELECT 1 FROM coachs c WHERE c.miembro_id = m.miembro_id)
            FROM miembros m
            ON CONFLICT (miembro_id) DO UPDATE SET 
                es_atleta = EXCLUDED.es_atleta,
                es_coach = EXCLUDED.es_coach;
        """))
        db.session.commit() 
        niveles_query = db.session.execute(text("""
            SELECT a.nivel, COUNT(*) as total 
            FROM dw.dim_miembros m
            JOIN atletas a ON m.miembro_id = a.miembro_id
            GROUP BY a.nivel
        """)).mappings().all()
        
        roles_query = db.session.execute(text("""
            SELECT 
                SUM(CASE WHEN es_atleta THEN 1 ELSE 0 END) as atletas,
                SUM(CASE WHEN es_coach THEN 1 ELSE 0 END) as coaches
            FROM dw.dim_miembros
        """)).mappings().first()

        return render_template("dashboard.html", niveles=niveles_query, roles=roles_query)

    return app
