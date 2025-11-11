-- Crear la base de datos si no existe
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'bi_juridico')
BEGIN
    CREATE DATABASE bi_juridico;
END
;

USE bi_juridico;
;

-- Crear tabla cliente
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cliente')
BEGIN
    CREATE TABLE cliente (
        ci VARCHAR(20) PRIMARY KEY,
        nombre VARCHAR(50) NOT NULL,
        apellido VARCHAR(50) NOT NULL
    );
END
;

-- Crear tabla casoJuridico
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'casoJuridico')
BEGIN
    CREATE TABLE casoJuridico (
        nrocaso VARCHAR(50) PRIMARY KEY,
        meteria VARCHAR(50) NOT NULL   -- coincide con el DataFrame (meteria)
    );
END
;

-- Crear tabla contratoServicio
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'contratoServicio')
BEGIN
    CREATE TABLE contratoServicio (
        nrocontrato VARCHAR(50) PRIMARY KEY,
        fecha DATE NOT NULL,
        monto DECIMAL(10,2) NOT NULL,
        cicliente VARCHAR(20) NOT NULL,
        nrocasojuridico VARCHAR(50),
        FOREIGN KEY (cicliente) REFERENCES cliente(ci)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        FOREIGN KEY (nrocasojuridico) REFERENCES casoJuridico(nrocaso)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );
END
;
